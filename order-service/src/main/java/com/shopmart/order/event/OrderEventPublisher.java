package com.shopmart.order.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Câu 3: Producer phát sự kiện Saga lên topic "order".
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publish(OrderEvent event) {
        log.info("[SAGA][PUBLISH] orderId={} type={} amount={} message={}",
                event.getOrderId(), event.getType(), event.getAmount(), event.getMessage());
        kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[SAGA][PUBLISH-FAIL] orderId={} type={} error={}",
                                event.getOrderId(), event.getType(), ex.getMessage());
                    }
                });
    }
}
