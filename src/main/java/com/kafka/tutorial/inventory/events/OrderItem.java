package com.kafka.tutorial.inventory.events;

public record OrderItem(
        String productId,
        Integer quantity
) {}