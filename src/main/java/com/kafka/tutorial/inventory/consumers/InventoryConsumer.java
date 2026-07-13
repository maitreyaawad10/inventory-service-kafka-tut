package com.kafka.tutorial.inventory.consumers;

import com.kafka.tutorial.inventory.events.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InventoryConsumer {

    @KafkaListener(topics = "orders")
    public void consume(OrderCreatedEvent orderCreatedEvent) {
        log.info("Received order: {}", orderCreatedEvent.orderId());
    }
}
