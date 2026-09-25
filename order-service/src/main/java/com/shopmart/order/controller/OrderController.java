package com.shopmart.order.controller;

import com.shopmart.order.client.InventoryGateway;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final InventoryGateway inventoryGateway;

    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody OrderRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(orderService.createOrder(request));
    }

    @GetMapping("/{id}")
    public OrderResponse getById(@PathVariable Long id) {
        return orderService.getOrderById(id);
    }

    @GetMapping
    public List<OrderResponse> getAll() {
        return orderService.getAllOrders();
    }

    // Câu 2: Endpoint gọi thử inventory-service qua FeignClient để minh chứng
    //        Load Balancing + Circuit Breaker (fallback khi service đích ngưng).
    @GetMapping("/products/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return inventoryGateway.getProduct(id);
    }

    /** Trả về instance (port) inventory-service đang phục vụ - minh chứng Load Balancing. */
    @GetMapping("/inventory-instance")
    public String inventoryInstance() {
        return inventoryGateway.instance();
    }
}
