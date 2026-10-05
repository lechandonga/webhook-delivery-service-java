package com.github.lechandonga.webhookdeliveryservicejava.web;

import com.github.lechandonga.webhookdeliveryservicejava.domain.DeliveryRecord;
import com.github.lechandonga.webhookdeliveryservicejava.domain.Subscription;
import com.github.lechandonga.webhookdeliveryservicejava.domain.WebhookEvent;
import com.github.lechandonga.webhookdeliveryservicejava.service.BackpressureException;
import com.github.lechandonga.webhookdeliveryservicejava.service.DuplicateEventException;
import com.github.lechandonga.webhookdeliveryservicejava.service.WebhookService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class WebhookController {

    private final WebhookService webhookService;

    public WebhookController(WebhookService webhookService) {
        this.webhookService = webhookService;
    }

    public record SubscribeRequest(String eventType, String callbackUrl) {
    }

    public record PublishRequest(String eventId, String eventType, String payload) {
    }

    public record DeliveryView(String eventId, String subscriptionId, String status, int attempts,
                               String lastError, String deadLetterReason) {
        static DeliveryView of(DeliveryRecord r) {
            return new DeliveryView(r.getEventId(), r.getSubscriptionId(), r.getStatus().name(),
                    r.getAttempts(), r.getLastError(), r.getDeadLetterReason());
        }
    }

    @PostMapping("/subscriptions")
    public ResponseEntity<Subscription> subscribe(@RequestBody SubscribeRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(webhookService.subscribe(request.eventType(), request.callbackUrl()));
    }

    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> publish(@RequestBody PublishRequest request) {
        WebhookEvent event = webhookService.publish(request.eventId(), request.eventType(), request.payload());
        return ResponseEntity.accepted().body(Map.of(
                "eventId", event.eventId(),
                "eventType", event.eventType(),
                "publishedAt", event.publishedAt().toString()));
    }

    @GetMapping("/events/{eventId}/deliveries")
    public ResponseEntity<List<DeliveryView>> deliveries(@PathVariable String eventId) {
        if (webhookService.findEvent(eventId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(webhookService.deliveriesOf(eventId).stream().map(DeliveryView::of).toList());
    }

    @GetMapping("/dead-letters")
    public List<DeliveryView> deadLetters() {
        return webhookService.deadLetters().stream().map(DeliveryView::of).toList();
    }

    @ExceptionHandler(DuplicateEventException.class)
    public ResponseEntity<Map<String, String>> duplicate(DuplicateEventException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "DUPLICATE_EVENT_CONFLICT", "message", e.getMessage()));
    }

    @ExceptionHandler(BackpressureException.class)
    public ResponseEntity<Map<String, String>> backpressure(BackpressureException e) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of("error", "BACKPRESSURE_LIMIT_EXCEEDED", "message", e.getMessage()));
    }
}
