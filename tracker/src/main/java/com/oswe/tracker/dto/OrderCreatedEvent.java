package com.oswe.tracker.dto;

import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        String userName,
        String itemName,
        int quantity
) {
    @Override
    public String toString() {
        return "OrderCreatedEvent{" +
                "orderId=" + orderId +
                ", userName='" + userName + '\'' +
                ", itemName='" + itemName + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
