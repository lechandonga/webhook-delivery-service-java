package com.github.lechandonga.webhookdeliveryservicejava.domain;

import java.time.Instant;
import java.util.UUID;

public record Subscription(String subscriptionId, String eventType, String callbackUrl, Instant createdAt) {

    public static Subscription of(String eventType, String callbackUrl) {
        return new Subscription(UUID.randomUUID().toString(), eventType, callbackUrl, Instant.now());
    }
}
