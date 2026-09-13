package com.oswe.producer.messaging.event;

import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        String userName,
        String itemName,
        int quantity
) {
}
