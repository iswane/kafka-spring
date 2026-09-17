package dev.iswane.orderservice.controller;

import dev.iswane.orderservice.dto.CreateOrderRequest;
import dev.iswane.orderservice.dto.OrderEvent;
import dev.iswane.orderservice.producer.OrderProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

import static org.springframework.http.HttpStatus.ACCEPTED;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderProducer producer;

    @PostMapping
    public ResponseEntity<Map<String, String>> placeOrder(
            @Valid @RequestBody CreateOrderRequest request
    ) {
        var orderId = UUID.randomUUID().toString();
        var event = OrderEvent.of(
                orderId,
                request.customerId(),
                request.productName(),
                request.quantity(),
                request.totalAmount()
        );

        producer.sendOrderEvent(event);
        log.info("Order placed successfully: {}", event.orderId());

        return ResponseEntity
                .status(ACCEPTED)
                .body(Map.of(
                        "orders", orderId,
                        "status", "CREATED",
                        "message", "Order placed successfully and under processing"
                ));
    }
}
