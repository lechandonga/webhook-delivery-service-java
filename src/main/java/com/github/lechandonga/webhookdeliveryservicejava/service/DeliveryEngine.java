package com.github.lechandonga.webhookdeliveryservicejava.service;

import com.github.lechandonga.webhookdeliveryservicejava.config.WebhookDeliveryProperties;
import com.github.lechandonga.webhookdeliveryservicejava.domain.DeliveryRecord;
import com.github.lechandonga.webhookdeliveryservicejava.domain.Subscription;
import com.github.lechandonga.webhookdeliveryservicejava.domain.WebhookEvent;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 投递引擎：每个订阅方一个单线程执行器，保证同一订阅方内按提交顺序投递；
 * 不同订阅方之间相互隔离、并行投递。全局待投递计数用于背压。
 */
@Component
public class DeliveryEngine {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEngine.class);

    private final WebhookDeliveryProperties properties;
    private final HttpClient httpClient;
    private final Map<String, ExecutorService> subscriberExecutors = new ConcurrentHashMap<>();
    private final AtomicInteger pendingCount = new AtomicInteger();

    public DeliveryEngine(WebhookDeliveryProperties properties) {
        this.properties = properties;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(properties.getConnectTimeoutMs()))
                .build();
    }

    public int pendingCount() {
        return pendingCount.get();
    }

    /** 背压检查：积压超限时拒绝。 */
    public void checkCapacity(int newDeliveries) {
        int pending = pendingCount.get();
        if (pending + newDeliveries > properties.getMaxPending()) {
            throw new BackpressureException(pending, properties.getMaxPending());
        }
    }

    public void submit(WebhookEvent event, Subscription subscription, DeliveryRecord record) {
        pendingCount.incrementAndGet();
        executorFor(subscription.subscriptionId()).submit(() -> {
            try {
                deliverWithRetry(event, subscription, record);
            } finally {
                pendingCount.decrementAndGet();
            }
        });
    }

    private ExecutorService executorFor(String subscriptionId) {
        return subscriberExecutors.computeIfAbsent(subscriptionId, id ->
                Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "delivery-" + id.substring(0, 8));
                    t.setDaemon(true);
                    return t;
                }));
    }

    private void deliverWithRetry(WebhookEvent event, Subscription subscription, DeliveryRecord record) {
        int maxAttempts = properties.getMaxAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(subscription.callbackUrl()))
                        .timeout(Duration.ofMillis(properties.getRequestTimeoutMs()))
                        .header("Content-Type", "application/json")
                        .header("X-Event-Id", event.eventId())
                        .header("X-Event-Type", event.eventType())
                        .POST(HttpRequest.BodyPublishers.ofString(event.payload()))
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    record.markDelivered();
                    log.info("投递成功 eventId={} subscription={} attempts={}",
                            event.eventId(), subscription.subscriptionId(), attempt);
                    return;
                }
                if (status >= 400 && status < 500) {
                    record.markAttempt("HTTP " + status);
                    record.markFailedPermanent("订阅方返回 4xx 业务性失败，不重试: HTTP " + status);
                    log.warn("投递永久失败(4xx) eventId={} subscription={} status={}",
                            event.eventId(), subscription.subscriptionId(), status);
                    return;
                }
                record.markAttempt("HTTP " + status);
                log.warn("投递失败(5xx)，将重试 eventId={} subscription={} attempt={}/{} status={}",
                        event.eventId(), subscription.subscriptionId(), attempt, maxAttempts, status);
            } catch (IOException | InterruptedException | IllegalArgumentException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                record.markAttempt(e.getClass().getSimpleName() + ": " + e.getMessage());
                log.warn("投递异常(超时/连接失败)，将重试 eventId={} subscription={} attempt={}/{} error={}",
                        event.eventId(), subscription.subscriptionId(), attempt, maxAttempts, e.getMessage());
            }
            if (attempt < maxAttempts) {
                sleep(backoffMs(attempt));
            }
        }
        record.markDeadLetter("重试次数耗尽(maxAttempts=" + maxAttempts + ")，最后错误: " + record.getLastError());
        log.error("进入死信 eventId={} subscription={} reason={}",
                event.eventId(), subscription.subscriptionId(), record.getDeadLetterReason());
    }

    /** 指数退避：baseMs * 2^(attempt-1)。 */
    long backoffMs(int attempt) {
        return properties.getBackoffBaseMs() * (1L << (attempt - 1));
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void shutdown() {
        subscriberExecutors.values().forEach(ExecutorService::shutdown);
        subscriberExecutors.values().forEach(executor -> {
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /** 仅供测试等待异步投递完成。 */
    public void awaitQuiescence(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (pendingCount.get() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        List<ExecutorService> executors = List.copyOf(subscriberExecutors.values());
        for (ExecutorService executor : executors) {
            try {
                executor.submit(() -> { }).get(30, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
