package com.github.lechandonga.webhookdeliveryservicejava.service;

import com.github.lechandonga.webhookdeliveryservicejava.config.WebhookProperties;
import com.github.lechandonga.webhookdeliveryservicejava.domain.Delivery;
import com.github.lechandonga.webhookdeliveryservicejava.domain.DeliveryStatus;
import com.github.lechandonga.webhookdeliveryservicejava.domain.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 投递调度器。
 * <p>每个订阅方拥有独立的有界队列与单线程 worker：
 * <ul>
 *     <li>同一订阅方严格按入队顺序投递（重试中的队首事件会挡住后续事件，不允许乱序超车）；</li>
 *     <li>不同订阅方的 worker 互不阻塞，单个订阅方持续失败不会拖慢其他订阅方；</li>
 *     <li>队列容量受限，容量由 {@code webhook.delivery.subscriber-queue-capacity} 控制，
 *     发布侧据此做背压拒绝。</li>
 * </ul>
 */
@Component
public class DeliveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(DeliveryScheduler.class);

    private record Task(EventRecord event, Delivery delivery, Instant readyAt) {
    }

    private final WebhookProperties properties;
    private final WebhookHttpClient httpClient;

    private final Map<String, LinkedBlockingDeque<Task>> queues = new ConcurrentHashMap<>();
    private final Map<String, Integer> running = new ConcurrentHashMap<>();

    public DeliveryScheduler(WebhookProperties properties, WebhookHttpClient httpClient) {
        this.properties = properties;
        this.httpClient = httpClient;
    }

    /**
     * 发布侧在持有自身锁的情况下预检容量：只要有一个目标订阅方队列已满就整体拒绝，
     * 避免出现部分入队的中间状态。
     */
    public void checkCapacity(Iterable<String> subscriberIds) {
        for (String subscriberId : subscriberIds) {
            LinkedBlockingDeque<Task> queue = queues.get(subscriberId);
            if (queue != null && queue.remainingCapacity() == 0) {
                throw new ApiException(org.springframework.http.HttpStatus.TOO_MANY_REQUESTS,
                        "BACKPRESSURE_QUEUE_FULL",
                        "订阅方 '" + subscriberId + "' 的待投递队列已满（容量 "
                                + properties.getSubscriberQueueCapacity()
                                + "），为防止内存积压拒绝本次发布，请稍后重试");
            }
        }
    }

    /**
     * 将一次投递加入对应订阅方队列尾部并确保 worker 存活。
     */
    public void enqueue(EventRecord event, Delivery delivery) {
        LinkedBlockingDeque<Task> queue = queues.computeIfAbsent(delivery.getSubscriberId(),
                key -> new LinkedBlockingDeque<>(properties.getSubscriberQueueCapacity()));
        queue.addLast(new Task(event, delivery, Instant.now()));
        ensureWorker(delivery.getSubscriberId(), queue);
    }

    public int queueSize(String subscriberId) {
        LinkedBlockingDeque<Task> queue = queues.get(subscriberId);
        return queue == null ? 0 : queue.size();
    }

    private void ensureWorker(String subscriberId, LinkedBlockingDeque<Task> queue) {
        if (running.putIfAbsent(subscriberId, 1) == null) {
            Thread worker = new Thread(() -> runLoop(subscriberId, queue),
                    "webhook-delivery-" + subscriberId);
            worker.setDaemon(true);
            worker.start();
            log.info("订阅方 '{}' 的投递 worker 已启动", subscriberId);
        }
    }

    private void runLoop(String subscriberId, LinkedBlockingDeque<Task> queue) {
        while (!Thread.currentThread().isInterrupted()) {
            Task task;
            try {
                task = queue.pollFirst(java.util.concurrent.TimeUnit.SECONDS.toMillis(2),
                        java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (task == null) {
                continue;
            }
            Task next = processTask(subscriberId, queue, task);
            if (next != null) {
                try {
                    queue.putFirst(next);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private Task processTask(String subscriberId, LinkedBlockingDeque<Task> queue, Task task) {
        Delivery delivery = task.delivery();
        EventRecord event = task.event();

        long waitMs = Duration.between(Instant.now(), task.readyAt()).toMillis();
        if (waitMs > 0) {
            log.info("订阅方 '{}' 的事件 {} 退避等待 {}ms 后进行第 {} 次尝试",
                    subscriberId, event.eventId(), waitMs, delivery.getAttempts() + 1);
            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queue.addFirst(task);
                return null;
            }
        }

        int attempt = delivery.getAttempts() + 1;
        delivery.setAttempts(attempt);
        delivery.setLastAttemptAt(Instant.now());
        delivery.setStatus(DeliveryStatus.IN_FLIGHT);

        AttemptResult result = httpClient.post(event, delivery, properties.getHttpTimeoutMs());

        if (result.success()) {
            delivery.setStatus(DeliveryStatus.SUCCEEDED);
            delivery.setLastError(null);
            delivery.setFinishedAt(Instant.now());
            log.info("事件 {} 投递给订阅方 '{}' 成功（第 {} 次尝试：{}）",
                    event.eventId(), subscriberId, attempt, result.detail());
            return null;
        }

        delivery.setLastError(result.detail());
        if (!result.retryable() || attempt >= properties.getMaxAttempts()) {
            delivery.setStatus(DeliveryStatus.DEAD);
            delivery.setFinishedAt(Instant.now());
            String reason = !result.retryable()
                    ? result.detail()
                    : "已达最大尝试次数 " + properties.getMaxAttempts() + "，最后一次失败原因：" + result.detail();
            delivery.setLastError(reason);
            log.warn("事件 {} 投递给订阅方 '{}' 进入死信：{}", event.eventId(), subscriberId, reason);
            return null;
        }

        long backoff = backoffMs(attempt);
        delivery.setStatus(DeliveryStatus.RETRYING);
        delivery.setNextAttemptAt(Instant.now().plusMillis(backoff));
        log.warn("事件 {} 投递给订阅方 '{}' 第 {} 次尝试失败（{}），{}ms 后重试",
                event.eventId(), subscriberId, attempt, result.detail(), backoff);
        return new Task(event, delivery, delivery.getNextAttemptAt());
    }

    private long backoffMs(int attempt) {
        long delay = (long) (properties.getBackoffInitialMs()
                * Math.pow(properties.getBackoffMultiplier(), attempt - 1));
        return Math.min(delay, properties.getBackoffMaxMs());
    }
}
