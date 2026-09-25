package com.shopmart.order.event;

import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu 3: Choreography Saga - order-service lắng nghe kết quả từ các bước sau.
 *
 * <ul>
 *   <li>{@code PAYMENT_COMPLETED} -> đơn COMPLETED (Saga thành công).</li>
 *   <li>{@code PAYMENT_FAILED}    -> huỷ đơn + phát {@code INVENTORY_RELEASED} để hoàn tồn kho
 *       (compensating transaction).</li>
 *   <li>{@code INVENTORY_FAILED}  -> huỷ đơn (chưa trừ kho nên không cần hoàn).</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaConsumer {

    private final OrderService orderService;
    private final OrderEventPublisher publisher;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(OrderEvent event) {
        log.info("[SAGA][CONSUME][order-service] orderId={} type={}", event.getOrderId(), event.getType());
        switch (event.getType()) {
            case PAYMENT_COMPLETED -> {
                orderService.completeOrder(event.getOrderId());
                log.info("[SAGA][DONE] Đơn hàng {} hoàn tất.", event.getOrderId());
            }
            case PAYMENT_FAILED -> {
                log.error("[SAGA][ROLLBACK] Thanh toán thất bại cho đơn {}: {}",
                        event.getOrderId(), event.getMessage());
                orderService.cancelOrder(event.getOrderId(), "Thanh toán thất bại: " + event.getMessage());
                publishReleaseStock(event);
            }
            case INVENTORY_FAILED -> {
                log.error("[SAGA][ROLLBACK] Giữ tồn kho thất bại cho đơn {}: {}",
                        event.getOrderId(), event.getMessage());
                orderService.cancelOrder(event.getOrderId(), "Không đủ tồn kho: " + event.getMessage());
            }
            default -> log.debug("[SAGA][SKIP] order-service bỏ qua type={}", event.getType());
        }
    }

    /** Phát lệnh hoàn tồn kho cho inventory-service. */
    private void publishReleaseStock(OrderEvent failedEvent) {
        OrderEvent compensation = OrderEvent.builder()
                .orderId(failedEvent.getOrderId())
                .productId(failedEvent.getProductId())
                .quantity(failedEvent.getQuantity())
                .amount(failedEvent.getAmount())
                .type(SagaEventType.INVENTORY_RELEASED)
                .message("Compensate: hoàn tồn kho do thanh toán thất bại")
                .build();
        publisher.publish(compensation);
    }
}
