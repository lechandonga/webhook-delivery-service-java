package com.github.lechandonga.webhookdeliveryservicejava.tests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端业务场景集成测试：服务内投递引擎 + 本地 HTTP 桩模拟订阅方。
 * 场景边界与执行结果通过 {@link ScenarioLoggingExtension} 打到日志。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(ScenarioLoggingExtension.class)
class WebhookDeliveryIntegrationTests {

    private static StubHttpServer stub;

    @Autowired
    private TestRestTemplate rest;

    @BeforeAll
    static void startStub() {
        stub = new StubHttpServer();
    }

    @AfterAll
    static void stopStub() {
        stub.close();
    }

    private void subscribe(String subscriberId, String eventType, String callbackPath) {
        Map<String, Object> body = Map.of(
                "subscriberId", subscriberId,
                "eventType", eventType,
                "callbackUrl", stub.url(callbackPath));
        ResponseEntity<Map> response = rest.postForEntity("/api/webhooks/subscriptions", body, Map.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode(), "订阅注册应当成功");
    }

    private ResponseEntity<Map> publish(String eventId, String eventType, String payload) {
        Map<String, Object> body = Map.of(
                "eventId", eventId,
                "eventType", eventType,
                "payload", payload);
        return rest.postForEntity("/api/webhooks/events", body, Map.class);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> queryDeliveries(String eventId) {
        return rest.getForObject("/api/webhooks/events/" + eventId + "/deliveries", List.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deliveryFor(String eventId, String subscriberId) {
        return queryDeliveries(eventId).stream()
                .filter(d -> subscriberId.equals(d.get("subscriberId")))
                .map(d -> (Map<String, Object>) d)
                .findFirst()
                .orElseThrow();
    }

    private boolean awaitDelivery(String eventId, String subscriberId, String expectedStatus) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (expectedStatus.equals(deliveryFor(eventId, subscriberId).get("status"))) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    @Test
    @DisplayName("场景1: 同一事件标识重复/并发发布幂等；内容一致返回原结果，内容不一致明确拒绝")
    void duplicatePublish_isIdempotent_conflictingContentRejected() throws Exception {
        subscribe("dup-sub", "order.created", "/ok");
        String eventId = "evt-dup-" + UUID.randomUUID();
        String payload = "{\"orderId\":\"A001\",\"amount\":100}";

        ResponseEntity<Map> first = publish(eventId, "order.created", payload);
        assertEquals(HttpStatus.OK, first.getStatusCode());
        assertEquals(Boolean.TRUE, first.getBody().get("created"), "首次发布 created=true");

        // 重复发布：字段顺序与空白不同但 JSON 语义一致
        ResponseEntity<Map> duplicate = publish(eventId, "order.created",
                "{ \"amount\": 100, \"orderId\": \"A001\" }");
        assertEquals(HttpStatus.OK, duplicate.getStatusCode());
        assertEquals(Boolean.FALSE, duplicate.getBody().get("created"),
                "内容一致的重复发布必须幂等，created=false 并返回原有投递");

        // 并发重复提交：全部成功，但只有一个真正创建
        int threads = 10;
        // 缓存型线程池：确保所有线程都能先在起跑闸门前 park，避免池容量与任务数相等导致的自死锁
        ExecutorService pool = Executors.newCachedThreadPool();
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentHashMap<Boolean, AtomicInteger> createdCounts = new ConcurrentHashMap<>();
        createdCounts.put(true, new AtomicInteger());
        createdCounts.put(false, new AtomicInteger());
        List<Callable<Void>> tasks = new ArrayList<>();
        String concurrentEventId = "evt-concurrent-" + UUID.randomUUID();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                start.await(10, TimeUnit.SECONDS);
                ResponseEntity<Map> r = publish(concurrentEventId, "order.created",
                        "{\"orderId\":\"C002\"}");
                createdCounts.get(Boolean.TRUE.equals(r.getBody().get("created"))).incrementAndGet();
                return null;
            });
        }
        List<java.util.concurrent.Future<Void>> futures = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            futures.add(pool.submit(task));
        }
        // 所有任务已进入队列并阻塞在起跑闸门上，再统一放行
        Thread.sleep(200);
        start.countDown();
        for (java.util.concurrent.Future<Void> future : futures) {
            future.get(15, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertEquals(1, createdCounts.get(true).get(), "并发发布中只能有一次真正创建事件");
        assertEquals(threads - 1, createdCounts.get(false).get(), "其余并发请求必须命中幂等");

        // 同一事件标识携带不一致内容：409 + 可区分错误码与原因
        ResponseEntity<Map> conflict = publish(eventId, "order.created", "{\"orderId\":\"A001\",\"amount\":999}");
        assertEquals(HttpStatus.CONFLICT, conflict.getStatusCode());
        assertEquals("EVENT_CONTENT_CONFLICT", conflict.getBody().get("code"));
        assertTrue(String.valueOf(conflict.getBody().get("message")).contains("内容不一致"),
        "拒绝响应必须说明原因");

        // 事件类型不一致同样拒绝
        ResponseEntity<Map> typeConflict = publish(eventId, "order.cancelled", payload);
        assertEquals(HttpStatus.CONFLICT, typeConflict.getStatusCode());
        assertEquals("EVENT_CONTENT_CONFLICT", typeConflict.getBody().get("code"));
    }

    @Test
    @DisplayName("场景2: 订阅方先返回5xx后恢复，按指数退避重试并最终成功")
    void transientServerError_retriesWithBackoffThenSucceeds() {
        String path = "/flaky-" + UUID.randomUUID();
        stub.setFlakyFailures(path, 2);
        subscribe("flaky-sub", "payment.succeeded", path);
        String eventId = "evt-flaky-" + UUID.randomUUID();

        publish(eventId, "payment.succeeded", "{\"paymentId\":\"P1\"}");

        assertTrue(awaitDelivery(eventId, "flaky-sub", "SUCCEEDED"), "重试后应投递成功");
        Map<String, Object> delivery = deliveryFor(eventId, "flaky-sub");
        assertEquals(3, ((Number) delivery.get("attempts")).intValue(),
                "前两次 5xx 重试，第三次成功，共 3 次尝试");
        assertEquals(3, stub.receivedEventIds(path).size(), "桩应实际收到 3 次 HTTP 投递");
    }

    @Test
    @DisplayName("场景3: 订阅方持续超时，重试次数耗尽后进入死信并记录原因")
    void alwaysTimeout_entersDeadLetterAfterMaxAttempts() {
        String path = "/slow-" + UUID.randomUUID();
        stub.setDelay(path, 800); // 大于测试配置的单次超时 400ms
        subscribe("slow-sub", "email.bounced", path);
        String eventId = "evt-slow-" + UUID.randomUUID();

        publish(eventId, "email.bounced", "{\"bounceId\":\"B1\"}");

        assertTrue(awaitDelivery(eventId, "slow-sub", "DEAD"), "重试耗尽应进入死信");
        Map<String, Object> delivery = deliveryFor(eventId, "slow-sub");
        assertEquals(3, ((Number) delivery.get("attempts")).intValue(), "最大尝试次数为 3");
        assertTrue(String.valueOf(delivery.get("lastError")).contains("最大尝试次数"),
                "死信必须可查到失败原因");
    }

    @Test
    @DisplayName("场景4: 4xx 业务性失败不重试，直接进入死信")
    void clientError_isNotRetried_goesStraightToDeadLetter() {
        subscribe("bad-sub", "invoice.issued", "/4xx-" + UUID.randomUUID());
        String eventId = "evt-4xx-" + UUID.randomUUID();

        publish(eventId, "invoice.issued", "{\"invoiceId\":\"I1\"}");

        assertTrue(awaitDelivery(eventId, "bad-sub", "DEAD"), "4xx 应直接进入死信");
        Map<String, Object> delivery = deliveryFor(eventId, "bad-sub");
        assertEquals(1, ((Number) delivery.get("attempts")).intValue(), "4xx 不允许任何重试");
        assertTrue(String.valueOf(delivery.get("lastError")).contains("4xx"), "失败原因应可解释");
    }

    @Test
    @DisplayName("场景5: 某订阅方持续失败不影响其他订阅方，且同一订阅方严格按序投递")
    void failingSubscriberIsolated_orderingPerSubscriberPreserved() throws Exception {
        // A：一直慢（超时并最终死信），其 worker 被占住；B：健康订阅方
        String slowPath = "/islow-" + UUID.randomUUID();
        stub.setDelay(slowPath, 800);
        subscribe("iso-slow", "stock.changed", slowPath);
        String okPath = "/ihealthy-" + UUID.randomUUID();
        subscribe("iso-healthy", "stock.changed", okPath);

        String[] healthyEvents = new String[3];
        for (int i = 0; i < 3; i++) {
            // 先发健康事件，再发慢事件，使健康订阅方的期望接收顺序与发布顺序一致
            publish("evt-iso-slow-" + i + "-" + UUID.randomUUID(), "stock.changed",
                    "{\"sku\":\"X" + i + "\"}");
            healthyEvents[i] = "evt-iso-h-" + i + "-" + UUID.randomUUID();
            publish(healthyEvents[i], "stock.changed", "{\"sku\":\"S" + i + "\"}");
        }

        // 健康订阅方必须在慢订阅方仍在退避/超时期间快速收齐全部事件
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline && stub.receivedEventIds(okPath).size() < 3) {
            Thread.sleep(50);
        }
        // 健康订阅方也会收到慢事件（同事件类型），筛选出健康事件并保持其到达顺序
        List<String> healthyReceived = stub.receivedEventIds(okPath).stream()
                .filter(java.util.Set.of(healthyEvents)::contains)
                .toList();
        assertEquals(3, healthyReceived.size(), "健康订阅方不应被持续失败的订阅方拖慢");
        for (int i = 0; i < 3; i++) {
            assertEquals(healthyEvents[i], healthyReceived.get(i), "同一订阅方必须按发布顺序投递");
        }

        // 慢订阅方在独立 worker 上继续按序重试，最终进入死信
        assertTrue(awaitDelivery(healthyEvents[0], "iso-slow", "DEAD"));
    }

    @Test
    @DisplayName("场景6: 待投递积压超过订阅方队列上限时拒绝发布，返回可区分的背压错误码")
    void backpressure_whenSubscriberQueueFull_rejectsNewPublish() {
        String path = "/slow-bp-" + UUID.randomUUID();
        stub.setDelay(path, 2_000); // worker 被首个事件长时间占住，后续事件在队列积压
        subscribe("bp-sub", "log.appended", path);

        // 测试配置队列容量为 10：1 个在投 + 10 个排队后，再发布必须被拒绝
        for (int i = 0; i < 11; i++) {
            ResponseEntity<Map> accepted = publish("evt-bp-" + i + "-" + UUID.randomUUID(),
                    "log.appended", "{\"seq\":" + i + "}");
            assertEquals(HttpStatus.OK, accepted.getStatusCode(), "未达上限前发布应被接受");
        }

        ResponseEntity<Map> rejected = publish("evt-bp-overflow-" + UUID.randomUUID(),
                "log.appended", "{\"seq\":99}");
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, rejected.getStatusCode(), "积压超限应拒绝发布");
        assertEquals("BACKPRESSURE_QUEUE_FULL", rejected.getBody().get("code"),
                "背压拒绝必须有可区分的错误码");
        assertTrue(String.valueOf(rejected.getBody().get("message")).contains("队列已满"),
                "拒绝原因应说明是哪个订阅方队列积压");
    }
}
