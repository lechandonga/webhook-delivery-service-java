package com.github.lechandonga.webhookdeliveryservicejava.domain;

import java.time.Instant;

/**
 * 一次事件到某个订阅方的投递记录，用于查询当前投递状态与失败原因。
 */
public class Delivery {

    private final String eventId;
    private final String eventType;
    private final String subscriberId;
    private final String callbackUrl;
    private volatile DeliveryStatus status = DeliveryStatus.PENDING;
    private volatile int attempts = 0;
    private volatile Instant lastAttemptAt;
    private volatile String lastError;
    private volatile Instant nextAttemptAt;
    private volatile Instant finishedAt;

    public Delivery(String eventId, String eventType, String subscriberId, String callbackUrl) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.subscriberId = subscriberId;
        this.callbackUrl = callbackUrl;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public void setStatus(DeliveryStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Instant getLastAttemptAt() {
        return lastAttemptAt;
    }

    public void setLastAttemptAt(Instant lastAttemptAt) {
        this.lastAttemptAt = lastAttemptAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(Instant finishedAt) {
        this.finishedAt = finishedAt;
    }
}
