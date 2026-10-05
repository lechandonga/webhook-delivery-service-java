package com.github.lechandonga.webhookdeliveryservicejava.tests;

import com.github.lechandonga.webhookdeliveryservicejava.service.DeliveryEngine;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集成测试：使用本地 HTTP 桩模拟订阅方的延迟与错误响应。
 * 场景覆盖：重复发布幂等、超时重试、永久失败进死信、订阅方之间互不影响、订阅方内顺序。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "webhook.delivery.max-attempts=3",
        "webhook.delivery.backoff-base-ms=100",
        "webhook.delivery.connect-timeout-ms=500",
        "webhook.delivery.request-timeout-ms=800",
        "webhook.delivery.max-pending=1000"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WebhookDeliveryIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryIntegrationTest.class);

    private static HttpServer stub;
    private static int stubPort;
    private static final AtomicInteger slowHits = new AtomicInteger();
    private static final AtomicInteger failHits = new AtomicInteger();
    private static final List<String> okReceivedEventIds = new CopyOnWriteArrayList<>();
    private static final Map<String, AtomicInteger> okHits = new ConcurrentHashMap<>();

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private DeliveryEngine deliveryEngine;

    @BeforeAll
    static void startStub() throws IOException {
        stub = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stubPort = stub.getAddress().getPort();
        stub.createContext("/ok", exchange -> {
            String eventId = exchange.getRequestHeaders().getFirst("X-Event-Id");
            okReceivedEventIds.add(eventId);
            okHits.computeIfAbsent(eventId, k -> new AtomicInteger()).incrementAndGet();
            respond(exchange, 200);
        });
        stub.createContext("/slow", exchange -> {
            slowHits.incrementAndGet();
            sleep(3000); // 超过 request-timeout-ms=800，触发超时
            respond(exchange, 200);
        });
        stub.createContext("/fail500", exchange -> {
            failHits.incrementAndGet();
            respond(exchange, 500);
        });
        stub.createContext("/fail400", exchange -> respond(exchange, 400));
        // 默认执行器是单线程顺序派发，/slow 会阻塞其他路径；用线程池模拟真实并发订阅方。
        stub.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        stub.start();
        log.info("HTTP 桩已启动 port={}", stubPort);
    }

    @AfterAll
    static void stopStub() {
        stub.stop(0);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status) throws IOException {
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String subscribe(String eventType, String path) {
        ResponseEntity<Map> resp = rest.postForEntity("/api/subscriptions",
                Map.of("eventType", eventType, "callbackUrl", "http://127.0.0.1:" + stubPort + path), Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return (String) resp.getBody().get("subscriptionId");
    }

    private ResponseEntity<Map> publish(String eventId, String eventType, String payload) {
        return rest.postForEntity("/api/events",
                Map.of("eventId", eventId, "eventType", eventType, "payload", payload), Map.class);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> deliveries(String eventId) {
        return rest.getForEntity("/api/events/" + eventId + "/deliveries", List.class).getBody();
    }

    @Test
    @Order(1)
    @DisplayName("场景1: 重复发布 - 内容一致幂等返回，内容不一致明确拒绝")
    void duplicatePublish() throws Exception {
        log.info("=== 场景1: 重复发布幂等性 ===");
        subscribe("order.created", "/ok");

        ResponseEntity<Map> first = publish("evt-dup-1", "order.created", "{\"a\":1}");
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        ResponseEntity<Map> same = publish("evt-dup-1", "order.created", "{\"a\":1}");
        assertThat(same.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(same.getBody().get("publishedAt")).isEqualTo(first.getBody().get("publishedAt"));

        ResponseEntity<Map> conflict = publish("evt-dup-1", "order.created", "{\"a\":2}");
        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(conflict.getBody().get("error")).isEqualTo("DUPLICATE_EVENT_CONFLICT");

        deliveryEngine.awaitQuiescence(5000);
        assertThat(okHits.get("evt-dup-1").get()).isEqualTo(1); // 只投递一次
        log.info("场景1 通过: 重复发布幂等返回原结果，不一致内容被拒绝(409)");
    }

    @Test
    @Order(2)
    @DisplayName("场景2: 订阅方超时 - 按退避策略重试，最终进死信")
    void timeoutRetryToDeadLetter() throws Exception {
        log.info("=== 场景2: 超时重试与死信 ===");
        subscribe("payment.slow", "/slow");
        int hitsBefore = slowHits.get();

        publish("evt-slow-1", "payment.slow", "{}");
        deliveryEngine.awaitQuiescence(30000);

        List<Map<String, Object>> records = deliveries("evt-slow-1");
        assertThat(records).hasSize(1);
        assertThat(records.get(0).get("status")).isEqualTo("DEAD_LETTER");
        assertThat(records.get(0).get("attempts")).isEqualTo(3);
        assertThat((String) records.get(0).get("deadLetterReason")).contains("重试次数耗尽");
        assertThat(slowHits.get() - hitsBefore).isEqualTo(3);

        List<Map<String, Object>> deadLetters = rest.getForEntity("/api/dead-letters", List.class).getBody();
        assertThat(deadLetters).anyMatch(d -> "evt-slow-1".equals(d.get("eventId")));
        log.info("场景2 通过: 超时按指数退避重试 3 次后进入死信，原因可查");
    }

    @Test
    @Order(3)
    @DisplayName("场景3: 4xx 业务性失败不重试，5xx 重试后进死信")
    void permanentVsRetryableFailure() throws Exception {
        log.info("=== 场景3: 4xx 不重试 / 5xx 重试 ===");
        subscribe("biz.rejected", "/fail400");
        subscribe("biz.error", "/fail500");
        int failHitsBefore = failHits.get();

        publish("evt-400-1", "biz.rejected", "{}");
        publish("evt-500-1", "biz.error", "{}");
        deliveryEngine.awaitQuiescence(30000);

        List<Map<String, Object>> r400 = deliveries("evt-400-1");
        assertThat(r400.get(0).get("status")).isEqualTo("FAILED_PERMANENT");
        assertThat(r400.get(0).get("attempts")).isEqualTo(1); // 不重试

        List<Map<String, Object>> r500 = deliveries("evt-500-1");
        assertThat(r500.get(0).get("status")).isEqualTo("DEAD_LETTER");
        assertThat(r500.get(0).get("attempts")).isEqualTo(3);
        assertThat(failHits.get() - failHitsBefore).isEqualTo(3);
        log.info("场景3 通过: 4xx 仅尝试 1 次标记永久失败，5xx 重试 3 次后进死信");
    }

    @Test
    @Order(4)
    @DisplayName("场景4: 订阅方隔离 - 持续失败的订阅方不拖慢其他订阅方，且同一订阅方内保序")
    void subscriberIsolationAndOrdering() throws Exception {
        log.info("=== 场景4: 订阅方隔离与顺序保证 ===");
        subscribe("isolated.event", "/fail500"); // 持续失败
        subscribe("isolated.event", "/ok");      // 正常
        okReceivedEventIds.clear();

        for (int i = 1; i <= 5; i++) {
            publish("evt-iso-" + i, "isolated.event", "{\"seq\":" + i + "}");
        }
        deliveryEngine.awaitQuiescence(30000);

        // 正常订阅方按发布顺序收到全部 5 个事件
        assertThat(okReceivedEventIds)
                .containsExactly("evt-iso-1", "evt-iso-2", "evt-iso-3", "evt-iso-4", "evt-iso-5");
        // 失败订阅方全部进死信
        for (int i = 1; i <= 5; i++) {
            List<Map<String, Object>> records = deliveries("evt-iso-" + i);
            assertThat(records).anySatisfy(r -> assertThat(r.get("status")).isEqualTo("DELIVERED"));
            assertThat(records).anySatisfy(r -> assertThat(r.get("status")).isEqualTo("DEAD_LETTER"));
        }
        log.info("场景4 通过: 失败订阅方进入死信不影响正常订阅方，同一订阅方内按发布顺序投递");
    }
}
