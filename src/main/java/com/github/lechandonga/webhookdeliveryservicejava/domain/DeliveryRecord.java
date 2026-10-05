package com.github.lechandonga.webhookdeliveryservicejava.domain;

import java.time.Instant;

public class DeliveryRecord {

    private final String eventId;
    private final String subscriptionId;
    private volatile DeliveryStatus status = DeliveryStatus.PENDING;
    private volatile int attempts;
    private volatile String lastError;
    private volatile String deadLetterReason;
    private volatile Instant updatedAt = Instant.now();

    public DeliveryRecord(String eventId, String subscriptionId) {
        this.eventId = eventId;
        this.subscriptionId = subscriptionId;
    }

    public void markAttempt(String error) {
        this.attempts++;
        this.lastError = error;
        this.updatedAt = Instant.now();
    }

    public void markDelivered() {
        this.status = DeliveryStatus.DELIVERED;
        this.lastError = null;
        this.updatedAt = Instant.now();
    }

    public void markFailedPermanent(String reason) {
        this.status = DeliveryStatus.FAILED_PERMANENT;
        this.lastError = reason;
        this.updatedAt = Instant.now();
    }

    public void markDeadLetter(String reason) {
        this.status = DeliveryStatus.DEAD_LETTER;
        this.deadLetterReason = reason;
        this.updatedAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastError() {
        return lastError;
    }

    public String getDeadLetterReason() {
        return deadLetterReason;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
