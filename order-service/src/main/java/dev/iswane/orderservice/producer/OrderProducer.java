package dev.iswane.orderservice.producer;

import dev.iswane.orderservice.dto.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;


@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    public CompletableFuture<SendResult<String, OrderEvent>> sendOrderEvent(OrderEvent orderEvent) {
        log.info("Sending order: {}, to topic: {}", orderEvent, ordersTopic);
        var future = kafkaTemplate.send(ordersTopic, orderEvent);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send order event: {}", orderEvent, ex);
            } else {
                var metadata = result.getRecordMetadata();
                log.info("Successfully sent order event: {} to partition: {} at offset: {}", orderEvent, metadata.partition(), metadata.offset());
            }
        });
        return future;
    }

}
