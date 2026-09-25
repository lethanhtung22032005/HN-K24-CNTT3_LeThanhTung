package com.shopmart.notification.config;

import com.shopmart.notification.event.KafkaTopics;
import com.shopmart.notification.event.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Câu 3 (Nâng cao): cấu hình Reactor Kafka consumer (reactive) cho notification-service.
 */
@Configuration
public class ReactiveKafkaConfig {

    @Bean
    public ReceiverOptions<String, OrderEvent> orderReceiverOptions(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "notification-reactive");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.shopmart.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());

        return ReceiverOptions.<String, OrderEvent>create(props)
                .subscription(Collections.singleton(KafkaTopics.ORDER));
    }

    @Bean
    public KafkaReceiver<String, OrderEvent> orderKafkaReceiver(
            ReceiverOptions<String, OrderEvent> orderReceiverOptions) {
        return KafkaReceiver.create(orderReceiverOptions);
    }
}
