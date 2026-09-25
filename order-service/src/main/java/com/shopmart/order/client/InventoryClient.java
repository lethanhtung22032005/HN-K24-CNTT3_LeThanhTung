package com.shopmart.order.client;

import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * Câu 2: FeignClient gọi inventory-service THEO TÊN (value = "inventory-service").
 * Nhờ Spring Cloud LoadBalancer + Eureka, Feign tự chọn instance còn sống
 * (nếu chạy 2 instance ở port 8082 và 8084 sẽ thấy request luân phiên).
 */
@FeignClient(name = "inventory-service", path = "/api/inventory")
public interface InventoryClient {

    @GetMapping("/products/{id}")
    ProductDto getProduct(@PathVariable("id") Long id);

    @PutMapping("/products/{id}/decrease")
    ProductDto decreaseStock(@PathVariable("id") Long id, @RequestBody StockRequest request);

    @PutMapping("/products/{id}/increase")
    ProductDto increaseStock(@PathVariable("id") Long id, @RequestBody StockRequest request);

    /** Trả về port của instance inventory đang phục vụ - minh chứng Load Balancing. */
    @GetMapping("/instance")
    String instance();
}
