package com.github.lechandonga.webhookdeliveryservicejava.tests;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地 HTTP 桩：模拟订阅方回调，支持正常 200、固定延迟（用于触发超时）、
 * 固定 5xx/4xx、先失败 N 次后恢复，以及记录投递顺序。
 */
public class StubHttpServer implements AutoCloseable {

    private final HttpServer server;

    /** 各路径的请求到达记录（按到达顺序，保存 eventId）。 */
    private final Map<String, List<String>> receivedEventIds = new ConcurrentHashMap<>();
    /** flaky 路径：先返回 500 的次数。 */
    private final Map<String, AtomicInteger> failRemaining = new ConcurrentHashMap<>();
    /** slow 路径：固定延迟毫秒。 */
    private final Map<String, Long> delayMs = new ConcurrentHashMap<>();

    public StubHttpServer() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException("无法启动本地 HTTP 桩", e);
        }
        // 默认分发器是单线程串行的，慢响应会堵住其他订阅方的桩请求；使用独立线程池模拟多个订阅方
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/", this::handle);
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public String url(String path) {
        return "http://127.0.0.1:" + port() + path;
    }

    public void setFlakyFailures(String path, int failures) {
        failRemaining.put(path, new AtomicInteger(failures));
    }

    public void setDelay(String path, long ms) {
        delayMs.put(path, ms);
    }

    public List<String> receivedEventIds(String path) {
        return receivedEventIds.getOrDefault(path, List.of());
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        exchange.getRequestBody().readAllBytes();
        String eventId = exchange.getRequestHeaders().getFirst("X-Event-Id");
        receivedEventIds.computeIfAbsent(path, key -> new CopyOnWriteArrayList<>()).add(eventId);

        Long delay = delayMs.get(path);
        if (delay != null) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        int status;
        if (path.startsWith("/5xx") || path.startsWith("/dead")) {
            status = 500;
        } else if (path.startsWith("/4xx")) {
            status = 400;
        } else if (path.startsWith("/flaky")) {
            AtomicInteger remaining = failRemaining.get(path);
            status = (remaining != null && remaining.getAndDecrement() > 0) ? 500 : 200;
        } else {
            status = 200;
        }
        byte[] body = ("stub:" + status).getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
