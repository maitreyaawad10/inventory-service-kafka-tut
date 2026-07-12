package com.kafka.tutorial.inventory.consumers;

import com.kafka.tutorial.inventory.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    @KafkaListener(
            topics="orders",
            groupId = "inventory-group"
    )
    public void consume(OrderCreatedEvent orderCreatedEvent) {
        log.info("Received order: {}", orderCreatedEvent.orderId());
    }
}
