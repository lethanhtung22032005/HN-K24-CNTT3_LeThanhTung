package com.shopmart.payment.event;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu 3: Choreography Saga - payment-service.
 * Nhận {@code INVENTORY_RESERVED} -> xử lý thanh toán -> phát
 * {@code PAYMENT_COMPLETED} hoặc {@code PAYMENT_FAILED}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSagaConsumer {

    private final PaymentService paymentService;
    private final OrderEventPublisher publisher;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(OrderEvent event) {
        log.info("[SAGA][CONSUME][payment-service] orderId={} type={}", event.getOrderId(), event.getType());
        if (event.getType() != SagaEventType.INVENTORY_RESERVED) {
            log.debug("[SAGA][SKIP] payment-service bỏ qua type={}", event.getType());
            return;
        }
        try {
            PaymentResponse response = paymentService.processPayment(
                    new PaymentRequest(event.getOrderId(), event.getAmount()));

            if (response.getStatus() == PaymentStatus.SUCCESS) {
                log.info("[SAGA] Thanh toán thành công cho đơn {}", event.getOrderId());
                publisher.publish(withType(event, SagaEventType.PAYMENT_COMPLETED, "Thanh toán thành công"));
            } else {
                log.error("[SAGA][ROLLBACK] Thanh toán thất bại cho đơn {}: {}",
                        event.getOrderId(), response.getMessage());
                publisher.publish(withType(event, SagaEventType.PAYMENT_FAILED, response.getMessage()));
            }
        } catch (IllegalStateException duplicate) {
            log.warn("[SAGA] Đơn {} đã được xử lý thanh toán trước đó, bỏ qua (idempotent)",
                    event.getOrderId());
        }
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
