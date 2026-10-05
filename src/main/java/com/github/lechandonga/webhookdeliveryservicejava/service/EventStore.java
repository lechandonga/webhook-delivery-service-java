package com.github.lechandonga.webhookdeliveryservicejava.service;

import com.github.lechandonga.webhookdeliveryservicejava.domain.Delivery;
import com.github.lechandonga.webhookdeliveryservicejava.domain.EventRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存事件与投递记录存储。
 */
@Component
public class EventStore {

    private final Map<String, EventRecord> events = new ConcurrentHashMap<>();
    private final Map<String, List<Delivery>> deliveriesByEvent = new ConcurrentHashMap<>();

    public void save(EventRecord event, List<Delivery> deliveries) {
        events.put(event.eventId(), event);
        deliveriesByEvent.put(event.eventId(), List.copyOf(deliveries));
    }

    public Optional<EventRecord> findEvent(String eventId) {
        return Optional.ofNullable(events.get(eventId));
    }

    public List<Delivery> findDeliveries(String eventId) {
        return deliveriesByEvent.getOrDefault(eventId, List.of());
    }
}
