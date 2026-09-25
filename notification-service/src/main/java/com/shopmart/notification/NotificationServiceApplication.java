package com.shopmart.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Câu 3 (Nâng cao): Consumer Kafka viết theo kiểu reactive (WebFlux + Reactor Kafka)
 * để minh chứng tư duy Async / Event-driven / Loose Coupling.
 */
@EnableDiscoveryClient
@SpringBootApplication
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
