package com.github.lechandonga.webhookdeliveryservicejava.domain;

import java.time.Instant;

public record WebhookEvent(String eventId, String eventType, String payload, Instant publishedAt) {
}
