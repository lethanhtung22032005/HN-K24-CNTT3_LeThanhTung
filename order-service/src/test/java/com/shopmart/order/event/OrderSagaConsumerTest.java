package com.shopmart.order.event;

import com.shopmart.order.service.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Câu 5: Unit test cho kịch bản ROLLBACK của Saga.
 * Khi nhận PAYMENT_FAILED -> đơn chuyển CANCELLED và phát INVENTORY_RELEASED (hoàn tồn kho).
 */
@ExtendWith(MockitoExtension.class)
class OrderSagaConsumerTest {

    @Mock
    private OrderService orderService;

    @Mock
    private OrderEventPublisher publisher;

    @InjectMocks
    private OrderSagaConsumer consumer;

    @Test
    void onPaymentFailed_shouldCancelOrderAndPublishInventoryReleased() {
        OrderEvent paymentFailed = OrderEvent.builder()
                .orderId(10L)
                .productId(3L)
                .quantity(3)
                .amount(new BigDecimal("84000000"))
                .type(SagaEventType.PAYMENT_FAILED)
                .message("Số tiền vượt hạn mức 80000000")
                .build();

        consumer.onEvent(paymentFailed);

        verify(orderService).cancelOrder(eq(10L), anyString());

        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(publisher).publish(captor.capture());
        OrderEvent compensation = captor.getValue();
        assertThat(compensation.getType()).isEqualTo(SagaEventType.INVENTORY_RELEASED);
        assertThat(compensation.getOrderId()).isEqualTo(10L);
        assertThat(compensation.getProductId()).isEqualTo(3L);
        assertThat(compensation.getQuantity()).isEqualTo(3);
    }

    @Test
    void onPaymentCompleted_shouldCompleteOrderWithoutCompensation() {
        OrderEvent paymentCompleted = OrderEvent.builder()
                .orderId(11L)
                .type(SagaEventType.PAYMENT_COMPLETED)
                .build();

        consumer.onEvent(paymentCompleted);

        verify(orderService).completeOrder(11L);
        verify(publisher, never()).publish(any());
    }

    @Test
    void onInventoryFailed_shouldCancelOrderWithoutCompensation() {
        OrderEvent inventoryFailed = OrderEvent.builder()
                .orderId(12L)
                .type(SagaEventType.INVENTORY_FAILED)
                .message("Không đủ tồn kho")
                .build();

        consumer.onEvent(inventoryFailed);

        verify(orderService).cancelOrder(eq(12L), anyString());
        verify(publisher, never()).publish(any());
    }
}
