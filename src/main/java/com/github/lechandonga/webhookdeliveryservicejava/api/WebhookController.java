package com.github.lechandonga.webhookdeliveryservicejava.api;

import com.github.lechandonga.webhookdeliveryservicejava.domain.Delivery;
import com.github.lechandonga.webhookdeliveryservicejava.domain.EventRecord;
import com.github.lechandonga.webhookdeliveryservicejava.domain.Subscription;
import com.github.lechandonga.webhookdeliveryservicejava.service.ApiException;
import com.github.lechandonga.webhookdeliveryservicejava.service.EventStore;
import com.github.lechandonga.webhookdeliveryservicejava.service.SubscriptionRegistry;
import com.github.lechandonga.webhookdeliveryservicejava.service.WebhookService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/webhooks")
public class WebhookController {

    private final WebhookService webhookService;
    private final EventStore eventStore;
    private final SubscriptionRegistry subscriptionRegistry;

    public WebhookController(WebhookService webhookService,
                             EventStore eventStore,
                             SubscriptionRegistry subscriptionRegistry) {
        this.webhookService = webhookService;
        this.eventStore = eventStore;
        this.subscriptionRegistry = subscriptionRegistry;
    }

    @PostMapping("/subscriptions")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, Object> register(@RequestBody SubscribeRequest request) {
        if (request == null || request.subscriberId() == null || request.subscriberId().isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_SUBSCRIBER_ID", "subscriberId 不能为空");
        }
        if (request.eventType() == null || request.eventType().isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_EVENT_TYPE", "eventType 不能为空");
        }
        if (request.callbackUrl() == null || request.callbackUrl().isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_CALLBACK_URL", "callbackUrl 不能为空");
        }
        SubscriptionRegistry.RegisterResult result = subscriptionRegistry.register(
                request.subscriberId(), request.eventType(), request.callbackUrl());
        Subscription s = result.subscription();
        return Map.of(
                "created", result.created(),
                "subscription", Map.of(
                        "subscriberId", s.subscriberId(),
                        "eventType", s.eventType(),
                        "callbackUrl", s.callbackUrl(),
                        "createdAt", s.createdAt().toString()));
    }

    @GetMapping("/subscriptions")
    public List<Subscription> listSubscriptions() {
        return subscriptionRegistry.findAll();
    }

    @PostMapping("/events")
    public Map<String, Object> publish(@RequestBody PublishRequest request) {
        if (request == null) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "请求体不能为空");
        }
        WebhookService.PublishOutcome outcome =
                webhookService.publish(request.eventId(), request.eventType(), request.payload());
        return Map.of(
                "eventId", request.eventId(),
                "created", outcome.created(),
                "deliveries", outcome.deliveries().stream().map(DeliveryView::new).toList());
    }

    @GetMapping("/events/{eventId}")
    public Map<String, Object> getEvent(@PathVariable String eventId) {
        EventRecord event = eventStore.findEvent(eventId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "EVENT_NOT_FOUND",
                        "事件 '" + eventId + "' 不存在"));
        return Map.of(
                "event", Map.of(
                        "eventId", event.eventId(),
                        "eventType", event.eventType(),
                        "payload", event.payload(),
                        "createdAt", event.createdAt().toString()),
                "deliveries", eventStore.findDeliveries(eventId).stream().map(DeliveryView::new).toList());
    }

    @GetMapping("/events/{eventId}/deliveries")
    public List<DeliveryView> getDeliveries(@PathVariable String eventId) {
        eventStore.findEvent(eventId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "EVENT_NOT_FOUND",
                        "事件 '" + eventId + "' 不存在"));
        return eventStore.findDeliveries(eventId).stream().map(DeliveryView::new).toList();
    }

    public record SubscribeRequest(String subscriberId, String eventType, String callbackUrl) {
    }

    public record PublishRequest(String eventId, String eventType, String payload) {
    }

    /** 投递状态视图，字段在并发更新下读取快照值。 */
    public record DeliveryView(String eventId,
                               String eventType,
                               String subscriberId,
                               String callbackUrl,
                               String status,
                               int attempts,
                               String lastAttemptAt,
                               String nextAttemptAt,
                               String finishedAt,
                               String lastError) {
        public DeliveryView(Delivery d) {
            this(d.getEventId(), d.getEventType(), d.getSubscriberId(), d.getCallbackUrl(),
                    d.getStatus().name(), d.getAttempts(),
                    d.getLastAttemptAt() == null ? null : d.getLastAttemptAt().toString(),
                    d.getNextAttemptAt() == null ? null : d.getNextAttemptAt().toString(),
                    d.getFinishedAt() == null ? null : d.getFinishedAt().toString(),
                    d.getLastError());
        }
    }
}
