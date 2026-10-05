package com.github.lechandonga.webhookdeliveryservicejava.service;

import com.github.lechandonga.webhookdeliveryservicejava.domain.Delivery;
import com.github.lechandonga.webhookdeliveryservicejava.domain.EventRecord;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;

/**
 * 基于 JDK HttpClient 的回调调用器，负责把失败原因归类为
 * 成功 / 可重试失败（超时、连接失败、5xx）/ 业务性失败（4xx）。
 */
@Component
public class WebhookHttpClient {

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    public AttemptResult post(EventRecord event, Delivery delivery, long timeoutMs) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(delivery.getCallbackUrl()))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json; charset=UTF-8")
                .header("X-Event-Id", event.eventId())
                .header("X-Event-Type", event.eventType())
                .header("X-Delivery-Attempt", String.valueOf(delivery.getAttempts() + 1))
                .POST(HttpRequest.BodyPublishers.ofString(event.payload()))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int code = response.statusCode();
            if (code >= 200 && code < 300) {
                return AttemptResult.ok(code);
            }
            if (code >= 500) {
                return AttemptResult.retryableFailure(code);
            }
            return AttemptResult.businessFailure(code);
        } catch (HttpTimeoutException e) {
            return AttemptResult.transportFailure("请求超时（>" + timeoutMs + "ms）");
        } catch (java.net.ConnectException e) {
            return AttemptResult.transportFailure("连接被拒绝/无法建立：" + e.getMessage());
        } catch (Exception e) {
            return AttemptResult.transportFailure("网络异常 " + e.getClass().getSimpleName()
                    + "：" + e.getMessage());
        }
    }
}
