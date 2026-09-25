package com.shopmart.notification.consumer;

import com.shopmart.notification.event.OrderEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.util.retry.Retry;

import java.time.Duration;

/**
 * Câu 3 (Nâng cao): Consumer Kafka kiểu reactive.
 *
 * <p>Luồng dữ liệu hoàn toàn bất đồng bộ: {@code KafkaReceiver.receive()}
 * trả về {@code Flux<ReceiverRecord>}, được xử lý qua các toán tử reactive
 * rồi đẩy vào một {@link Sinks.Many} để phát tiếp cho client qua SSE.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveOrderEventConsumer {

    private final KafkaReceiver<String, OrderEvent> orderKafkaReceiver;

    /** Hot sink phát sự kiện tới các subscriber (SSE). */
    private final Sinks.Many<OrderEvent> sink = Sinks.many().multicast().onBackpressureBuffer();

    private Disposable subscription;

    @PostConstruct
    public void start() {
        subscription = orderKafkaReceiver.receive()
                .doOnNext(record -> log.info("[REACTIVE][RECEIVE] partition={} offset={} key={}",
                        record.partition(), record.offset(), record.key()))
                .map(record -> record.value())
                .doOnNext(event -> log.info("[REACTIVE][CONSUME] orderId={} type={} amount={} message={}",
                        event.getOrderId(), event.getType(), event.getAmount(), event.getMessage()))
                .doOnNext(sink::tryEmitNext)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .doBeforeRetry(signal -> log.warn("[REACTIVE][RETRY] Kết nối lại Kafka: {}",
                                signal.failure().getMessage())))
                .subscribe(
                        null,
                        error -> log.error("[REACTIVE][ERROR] {}", error.getMessage()));
        log.info("[REACTIVE] Notification consumer đã khởi động (WebFlux + Reactor Kafka)");
    }

    /** Cho phép client subscribe sự kiện qua Server-Sent Events. */
    public Flux<OrderEvent> stream() {
        return sink.asFlux();
    }

    @jakarta.annotation.PreDestroy
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
