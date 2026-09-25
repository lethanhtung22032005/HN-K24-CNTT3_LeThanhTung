package com.shopmart.notification.controller;

import com.shopmart.notification.consumer.ReactiveOrderEventConsumer;
import com.shopmart.notification.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * Câu 3 (Nâng cao): expose luồng sự kiện reactive qua Server-Sent Events.
 * Ví dụ: {@code curl -N http://localhost:8085/api/notification/events}
 */
@RestController
@RequestMapping("/api/notification")
@RequiredArgsConstructor
public class NotificationController {

    private final ReactiveOrderEventConsumer consumer;

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<OrderEvent> events() {
        return consumer.stream();
    }

    @GetMapping("/health")
    public String health() {
        return "notification-service (WebFlux) is running";
    }
}
