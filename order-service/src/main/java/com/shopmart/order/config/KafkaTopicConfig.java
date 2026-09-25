package com.shopmart.order.config;

import com.shopmart.order.event.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Câu 3: Tạo topic "order" khi ứng dụng khởi động (nếu Kafka chưa có).
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
