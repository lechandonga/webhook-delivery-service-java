package com.github.lechandonga.webhookdeliveryservicejava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.webhookdeliveryservicejava.domain.Delivery;
import com.github.lechandonga.webhookdeliveryservicejava.domain.EventRecord;
import com.github.lechandonga.webhookdeliveryservicejava.domain.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 事件发布编排：幂等判重、内容一致性校验、背压预检与扇出。
 */
@Service
public class WebhookService {

    private static final Logger log = LoggerFactory.getLogger(WebhookService.class);

    private final SubscriptionRegistry subscriptionRegistry;
    private final EventStore eventStore;
    private final DeliveryScheduler scheduler;
    private final ObjectMapper objectMapper;

    /** 发布全流程串行化，保证并发重复提交时只有一个请求真正创建投递。 */
    private final Object publishLock = new Object();

    public WebhookService(SubscriptionRegistry subscriptionRegistry,
                          EventStore eventStore,
                          DeliveryScheduler scheduler,
                          ObjectMapper objectMapper) {
        this.subscriptionRegistry = subscriptionRegistry;
        this.eventStore = eventStore;
        this.scheduler = scheduler;
        this.objectMapper = objectMapper;
    }

    public PublishOutcome publish(String eventId, String eventType, String payload) {
        if (eventId == null || eventId.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_EVENT_ID", "eventId 不能为空");
        }
        if (eventType == null || eventType.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_EVENT_TYPE", "eventType 不能为空");
        }
        if (payload == null || payload.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_PAYLOAD", "payload 不能为空");
        }
        validateJson(payload);

        synchronized (publishLock) {
            EventRecord existing = eventStore.findEvent(eventId).orElse(null);
            if (existing != null) {
                if (!existing.eventType().equals(eventType) || !sameJson(existing.payload(), payload)) {
                    throw new ApiException(HttpStatus.CONFLICT, "EVENT_CONTENT_CONFLICT",
                            "事件标识 '" + eventId + "' 已存在但内容不一致（eventType 或 payload 不同），"
                                    + "同一事件标识必须携带完全相同的内容");
                }
                log.info("事件 {} 为重复发布（内容一致），返回原有投递结果", eventId);
                return new PublishOutcome(false, eventStore.findDeliveries(eventId));
            }

            List<Subscription> targets = subscriptionRegistry.findByEventType(eventType);
            List<String> targetIds = targets.stream().map(Subscription::subscriberId).toList();
            scheduler.checkCapacity(targetIds);

            EventRecord event = new EventRecord(eventId, eventType, payload, Instant.now());
            List<Delivery> deliveries = new ArrayList<>();
            for (Subscription subscription : targets) {
                deliveries.add(new Delivery(eventId, eventType,
                        subscription.subscriberId(), subscription.callbackUrl()));
            }
            eventStore.save(event, deliveries);
            for (int i = 0; i < targets.size(); i++) {
                scheduler.enqueue(event, deliveries.get(i));
            }
            log.info("事件 {} 首次发布，扇出给 {} 个订阅方: {}", eventId, targets.size(), targetIds);
            return new PublishOutcome(true, deliveries);
        }
    }

    private void validateJson(String payload) {
        try {
            objectMapper.readTree(payload);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_PAYLOAD",
                    "payload 不是合法 JSON：" + e.getOriginalMessage());
        }
    }

    /** 按 JSON 语义比较，而非字符串字面量，字段顺序/空白差异不影响判等。 */
    private boolean sameJson(String a, String b) {
        try {
            JsonNode nodeA = objectMapper.readTree(a);
            JsonNode nodeB = objectMapper.readTree(b);
            return nodeA.equals(nodeB);
        } catch (Exception e) {
            return false;
        }
    }

    public record PublishOutcome(boolean created, List<Delivery> deliveries) {
    }
}
