package com.github.lechandonga.webhookdeliveryservicejava.domain;

import java.time.Instant;

/**
 * 已发布的事件。
 *
 * @param eventId   调用方指定的幂等键，重复发布以此判重
 * @param eventType 事件类型，决定投递给哪些订阅方
 * @param payload   事件内容（JSON 原文），重复发布时用于内容一致性校验
 * @param createdAt 首次发布时间
 */
public record EventRecord(String eventId,
                          String eventType,
                          String payload,
                          Instant createdAt) {
}
