package dev.iswane.notificationservice.consumer;

import dev.iswane.notificationservice.dto.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.NonNull;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationConsumer {

    @KafkaListener(
            topics = "${kafka.topic.orders}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderEvent(
            @NonNull ConsumerRecord<String, OrderEvent> record,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        var event = record.value();
        log.info("""
                        Received new order event"
                        Topic: {},
                        Partition: {},
                        Offset: {},
                        Key: {},
                        OrderId: {},
                        Customer: {},
                        Product: {},
                        Amount: {},
                        Status: {},
                        """,
                record.topic(),
                partition,
                offset,
                record.key(),
                event.orderId(),
                event.customerId(),
                event.productName(),
                event.totalAmount(),
                event.status());

        processOrderEvent(event);

    }

    private void processOrderEvent(OrderEvent event) {
        log.info("Processing order event for order ID: {}", event.orderId());
    }
}
