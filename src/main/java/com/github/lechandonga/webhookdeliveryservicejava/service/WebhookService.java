package com.github.lechandonga.webhookdeliveryservicejava.service;

import com.github.lechandonga.webhookdeliveryservicejava.domain.DeliveryRecord;
import com.github.lechandonga.webhookdeliveryservicejava.domain.Subscription;
import com.github.lechandonga.webhookdeliveryservicejava.domain.WebhookEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class WebhookService {

    private static final Logger log = LoggerFactory.getLogger(WebhookService.class);

    private final DeliveryEngine deliveryEngine;
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, WebhookEvent> events = new ConcurrentHashMap<>();
    private final Map<String, String> eventFingerprints = new ConcurrentHashMap<>();
    private final Map<String, List<DeliveryRecord>> deliveryRecords = new ConcurrentHashMap<>();
    private final List<DeliveryRecord> deadLetters = new CopyOnWriteArrayList<>();

    public WebhookService(DeliveryEngine deliveryEngine) {
        this.deliveryEngine = deliveryEngine;
    }

    public Subscription subscribe(String eventType, String callbackUrl) {
        Subscription subscription = Subscription.of(eventType, callbackUrl);
        subscriptions.put(subscription.subscriptionId(), subscription);
        log.info("注册订阅 subscription={} eventType={} callbackUrl={}",
                subscription.subscriptionId(), eventType, callbackUrl);
        return subscription;
    }

    /**
     * 幂等发布：同一 eventId 内容一致时返回已有事件；内容不一致时抛出
     * {@link DuplicateEventException}。并发提交通过 compute 原子化。
     */
    public WebhookEvent publish(String eventId, String eventType, String payload) {
        String fingerprint = fingerprint(eventType, payload);
        // 背压检查放在事件落库之前，避免拒绝的发布留下无投递记录的残留事件。
        deliveryEngine.checkCapacity((int) subscriptions.values().stream()
                .filter(s -> s.eventType().equals(eventType)).count());
        WebhookEvent candidate = new WebhookEvent(eventId, eventType, payload, Instant.now());
        WebhookEvent stored = events.compute(eventId, (id, existing) -> {
            if (existing != null) {
                if (!eventFingerprints.get(id).equals(fingerprint)) {
                    throw new DuplicateEventException(eventId);
                }
                return existing;
            }
            eventFingerprints.put(id, fingerprint);
            return candidate;
        });
        if (stored != candidate) {
            log.info("重复发布(内容一致)，返回原事件 eventId={}", eventId);
            return stored;
        }
        dispatch(stored);
        return stored;
    }

    private void dispatch(WebhookEvent event) {
        List<Subscription> targets = subscriptions.values().stream()
                .filter(s -> s.eventType().equals(event.eventType()))
                .toList();
        List<DeliveryRecord> records = targets.stream()
                .map(s -> new DeliveryRecord(event.eventId(), s.subscriptionId()))
                .toList();
        deliveryRecords.put(event.eventId(), new CopyOnWriteArrayList<>(records));
        for (int i = 0; i < targets.size(); i++) {
            deliveryEngine.submit(event, targets.get(i), records.get(i));
        }
        log.info("事件已发布 eventId={} eventType={} 目标订阅数={}", event.eventId(), event.eventType(), targets.size());
    }

    public Optional<WebhookEvent> findEvent(String eventId) {
        return Optional.ofNullable(events.get(eventId));
    }

    public List<DeliveryRecord> deliveriesOf(String eventId) {
        return deliveryRecords.getOrDefault(eventId, List.of());
    }

    public List<DeliveryRecord> deadLetters() {
        refreshDeadLetters();
        return List.copyOf(deadLetters);
    }

    private void refreshDeadLetters() {
        deliveryRecords.values().forEach(records -> records.forEach(record -> {
            if (record.getStatus() == com.github.lechandonga.webhookdeliveryservicejava.domain.DeliveryStatus.DEAD_LETTER
                    && !deadLetters.contains(record)) {
                deadLetters.add(record);
            }
        }));
    }

    private static String fingerprint(String eventType, String payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(eventType.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            return HexFormat.of().formatHex(digest.digest(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
