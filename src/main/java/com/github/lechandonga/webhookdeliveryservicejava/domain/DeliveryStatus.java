package com.github.lechandonga.webhookdeliveryservicejava.domain;

public enum DeliveryStatus {
    /** 待投递或重试中。 */
    PENDING,
    /** 投递成功（订阅方返回 2xx）。 */
    DELIVERED,
    /** 业务性失败（订阅方返回 4xx），不重试。 */
    FAILED_PERMANENT,
    /** 重试次数耗尽，进入死信。 */
    DEAD_LETTER
}
