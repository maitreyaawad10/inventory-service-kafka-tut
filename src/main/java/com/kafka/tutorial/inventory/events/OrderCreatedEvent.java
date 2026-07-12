package com.kafka.tutorial.inventory.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        List<OrderItem> items,
        BigDecimal totalAmount,
        Instant createdAt
) {}