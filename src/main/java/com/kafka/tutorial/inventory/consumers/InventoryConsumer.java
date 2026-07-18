package com.kafka.tutorial.inventory.consumers;

import com.kafka.tutorial.inventory.events.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InventoryConsumer {

    @KafkaListener(topics = "orders")
    public void consume(OrderCreatedEvent orderCreatedEvent, Acknowledgment acknowledgment) {
        if (orderCreatedEvent.customerId() == null)
            throw new IllegalArgumentException("Customer Id is null");
        if (orderCreatedEvent.totalAmount() == null)
            throw new IllegalStateException("Total amount is null");
        log.info("Received order: {}", orderCreatedEvent.orderId());
        acknowledgment.acknowledge();
    }
}
