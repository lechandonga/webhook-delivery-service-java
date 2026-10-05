package com.github.lechandonga.webhookdeliveryservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 投递服务可配置项，前缀 {@code webhook.delivery}。
 */
@ConfigurationProperties(prefix = "webhook.delivery")
public class WebhookProperties {

    /** 单次 HTTP 请求超时（毫秒），超时视为可重试失败。 */
    private long httpTimeoutMs = 5_000;

    /** 最大投递尝试次数（首投 + 重试），耗尽后进入死信。 */
    private int maxAttempts = 5;

    /** 指数退避初始间隔（毫秒）。 */
    private long backoffInitialMs = 1_000;

    /** 指数退避倍率。 */
    private double backoffMultiplier = 2.0;

    /** 指数退避最大间隔（毫秒）。 */
    private long backoffMaxMs = 60_000;

    /** 每个订阅方的待投递队列容量，超过则新发布被拒绝（背压）。 */
    private int subscriberQueueCapacity = 200;

    public long getHttpTimeoutMs() {
        return httpTimeoutMs;
    }

    public void setHttpTimeoutMs(long httpTimeoutMs) {
        this.httpTimeoutMs = httpTimeoutMs;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffInitialMs() {
        return backoffInitialMs;
    }

    public void setBackoffInitialMs(long backoffInitialMs) {
        this.backoffInitialMs = backoffInitialMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public long getBackoffMaxMs() {
        return backoffMaxMs;
    }

    public void setBackoffMaxMs(long backoffMaxMs) {
        this.backoffMaxMs = backoffMaxMs;
    }

    public int getSubscriberQueueCapacity() {
        return subscriberQueueCapacity;
    }

    public void setSubscriberQueueCapacity(int subscriberQueueCapacity) {
        this.subscriberQueueCapacity = subscriberQueueCapacity;
    }
}
