package com.shopmart.order.service.impl;

import com.shopmart.order.client.InventoryGateway;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.OrderEventPublisher;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final InventoryGateway inventoryGateway;
    private final OrderEventPublisher eventPublisher;

    @Override
    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        // Câu 2: Gọi inventory-service qua FeignClient (đã bọc Circuit Breaker + fallback)
        //        để lấy thông tin sản phẩm -> tính totalAmount = price * quantity.
        ProductDto product = inventoryGateway.getProduct(request.getProductId());
        BigDecimal totalAmount = product.getPrice() != null
                ? product.getPrice().multiply(BigDecimal.valueOf(request.getQuantity()))
                : BigDecimal.ZERO;

        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .totalAmount(totalAmount)
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Created order id={} with status PENDING, totalAmount={}", saved.getId(), totalAmount);

        // Câu 3: Publish OrderEvent (type = ORDER_CREATED) lên Kafka topic "order" để khởi động Saga
        OrderEvent event = OrderEvent.builder()
                .orderId(saved.getId())
                .productId(saved.getProductId())
                .quantity(saved.getQuantity())
                .amount(totalAmount)
                .type(SagaEventType.ORDER_CREATED)
                .message("Đơn hàng mới được tạo")
                .build();
        eventPublisher.publish(event);

        return OrderResponse.from(saved);
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("Order id={} COMPLETED", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("Order id={} CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy đơn hàng id=" + id));
    }
}
