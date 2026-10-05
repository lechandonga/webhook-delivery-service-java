package com.github.lechandonga.webhookdeliveryservicejava.service;

import com.github.lechandonga.webhookdeliveryservicejava.domain.Subscription;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存订阅注册表。键为（订阅方标识, 事件类型）。
 */
@Component
public class SubscriptionRegistry {

    private record Key(String subscriberId, String eventType) {
    }

    private final Map<Key, Subscription> subscriptions = new ConcurrentHashMap<>();

    /**
     * 注册或复用订阅。
     *
     * @return 已存在且内容一致时返回原有订阅（{@code created=false}），否则新建
     * @throws ApiException 同一标识已注册到不同回调地址时返回 409
     */
    public synchronized RegisterResult register(String subscriberId, String eventType, String callbackUrl) {
        Key key = new Key(subscriberId, eventType);
        Subscription existing = subscriptions.get(key);
        if (existing != null) {
            if (!existing.callbackUrl().equals(callbackUrl)) {
                throw new ApiException(HttpStatus.CONFLICT, "SUBSCRIPTION_CONFLICT",
                        "订阅方 '" + subscriberId + "' 的事件类型 '" + eventType
                                + "' 已注册到不同回调地址: " + existing.callbackUrl());
            }
            return new RegisterResult(existing, false);
        }
        Subscription subscription = new Subscription(subscriberId, eventType, callbackUrl, Instant.now());
        subscriptions.put(key, subscription);
        return new RegisterResult(subscription, true);
    }

    public List<Subscription> findByEventType(String eventType) {
        return subscriptions.values().stream()
                .filter(s -> s.eventType().equals(eventType))
                .toList();
    }

    public List<Subscription> findAll() {
        return List.copyOf(subscriptions.values());
    }

    public Optional<Subscription> find(String subscriberId, String eventType) {
        return Optional.ofNullable(subscriptions.get(new Key(subscriberId, eventType)));
    }

    public record RegisterResult(Subscription subscription, boolean created) {
    }
}
