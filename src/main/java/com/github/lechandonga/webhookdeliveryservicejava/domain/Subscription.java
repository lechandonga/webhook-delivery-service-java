package com.github.lechandonga.webhookdeliveryservicejava.domain;

import java.time.Instant;

/**
 * 订阅关系：订阅方按事件类型注册的本地 HTTP 回调。
 *
 * @param subscriberId 订阅方标识，决定顺序投递与队列隔离的边界
 * @param eventType    订阅的事件类型
 * @param callbackUrl  事件投递的本地回调地址
 * @param createdAt    注册时间
 */
public record Subscription(String subscriberId,
                           String eventType,
                           String callbackUrl,
                           Instant createdAt) {
}
