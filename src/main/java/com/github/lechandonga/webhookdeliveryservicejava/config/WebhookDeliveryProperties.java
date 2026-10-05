package com.github.lechandonga.webhookdeliveryservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "webhook.delivery")
public class WebhookDeliveryProperties {

    /** 最大重试次数（首次投递不计入）。 */
    private int maxAttempts = 3;

    /** 退避基数（毫秒），第 n 次重试等待 baseMs * 2^(n-1)。 */
    private long backoffBaseMs = 200;

    /** 连接超时（毫秒）。 */
    private long connectTimeoutMs = 2000;

    /** 请求（响应）超时（毫秒）。 */
    private long requestTimeoutMs = 3000;

    /** 全局待投递事件上限，超过后拒绝新的发布（背压）。 */
    private int maxPending = 10000;

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffBaseMs() {
        return backoffBaseMs;
    }

    public void setBackoffBaseMs(long backoffBaseMs) {
        this.backoffBaseMs = backoffBaseMs;
    }

    public long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int getMaxPending() {
        return maxPending;
    }

    public void setMaxPending(int maxPending) {
        this.maxPending = maxPending;
    }
}
