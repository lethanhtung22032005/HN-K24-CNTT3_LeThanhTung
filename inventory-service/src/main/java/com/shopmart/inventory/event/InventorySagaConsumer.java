package com.shopmart.inventory.event;

import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu 3: Choreography Saga - inventory-service.
 *
 * <ul>
 *   <li>{@code ORDER_CREATED}     -> trừ tồn kho; thành công phát {@code INVENTORY_RESERVED},
 *       thất bại (hết hàng) phát {@code INVENTORY_FAILED}.</li>
 *   <li>{@code INVENTORY_RELEASED} -> compensating transaction: hoàn tồn kho khi
 *       bước thanh toán phía sau thất bại.</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventorySagaConsumer {

    private final ProductService productService;
    private final OrderEventPublisher publisher;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(OrderEvent event) {
        log.info("[SAGA][CONSUME][inventory-service] orderId={} type={}", event.getOrderId(), event.getType());
        switch (event.getType()) {
            case ORDER_CREATED -> reserveStock(event);
            case INVENTORY_RELEASED -> releaseStock(event);
            default -> log.debug("[SAGA][SKIP] inventory-service bỏ qua type={}", event.getType());
        }
    }

    private void reserveStock(OrderEvent event) {
        try {
            productService.decreaseStock(event.getProductId(), event.getQuantity());
            log.info("[SAGA] Đã giữ {} sản phẩm id={} cho đơn {}",
                    event.getQuantity(), event.getProductId(), event.getOrderId());
            publisher.publish(withType(event, SagaEventType.INVENTORY_RESERVED, "Đã giữ tồn kho"));
        } catch (Exception e) {
            log.error("[SAGA][ROLLBACK] Không giữ được tồn kho cho đơn {}: {}",
                    event.getOrderId(), e.getMessage());
            publisher.publish(withType(event, SagaEventType.INVENTORY_FAILED, e.getMessage()));
        }
    }

    private void releaseStock(OrderEvent event) {
        productService.increaseStock(event.getProductId(), event.getQuantity());
        log.info("[SAGA][COMPENSATE] Đã hoàn {} sản phẩm id={} cho đơn {}",
                event.getQuantity(), event.getProductId(), event.getOrderId());
    }

    private OrderEvent withType(OrderEvent source, SagaEventType type, String message) {
        return OrderEvent.builder()
                .orderId(source.getOrderId())
                .productId(source.getProductId())
                .quantity(source.getQuantity())
                .amount(source.getAmount())
                .type(type)
                .message(message)
                .build();
    }
}
