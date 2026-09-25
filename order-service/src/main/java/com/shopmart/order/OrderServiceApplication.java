package com.shopmart.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

// Câu 1: Đăng ký service vào Eureka
// Câu 2: Bật FeignClient (gọi inventory-service theo tên)
@EnableDiscoveryClient
@EnableFeignClients
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
