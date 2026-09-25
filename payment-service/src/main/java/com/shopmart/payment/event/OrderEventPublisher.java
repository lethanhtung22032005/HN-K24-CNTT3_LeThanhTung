package com.shopmart.payment.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Câu 3: Producer phát sự kiện Saga lên topic "order" từ payment-service.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publish(OrderEvent event) {
        log.info("[SAGA][PUBLISH] orderId={} type={} message={}",
                event.getOrderId(), event.getType(), event.getMessage());
        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[SAGA][PUBLISH-FAIL] orderId={} type={} error={}",
                                event.getOrderId(), event.getType(), ex.getMessage());
                    }
                });
    }
}
