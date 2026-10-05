package com.github.lechandonga.webhookdeliveryservicejava.domain;

/**
 * 单个（事件, 订阅方）的投递状态。
 * <ul>
 *     <li>{@link #PENDING}：已入队，等待投递；</li>
 *     <li>{@link #IN_FLIGHT}：正在调用订阅方回调；</li>
 *     <li>{@link #RETRYING}：本次尝试失败且可重试，退避后继续投递；</li>
 *     <li>{@link #SUCCEEDED}：订阅方返回 2xx；</li>
 *     <li>{@link #DEAD}：重试耗尽或业务性失败（4xx），进入死信。</li>
 * </ul>
 */
public enum DeliveryStatus {
    PENDING,
    IN_FLIGHT,
    RETRYING,
    SUCCEEDED,
    DEAD
}
