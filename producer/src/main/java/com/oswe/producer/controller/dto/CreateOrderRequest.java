package com.oswe.producer.controller.dto;

import java.util.UUID;

public record CreateOrderRequest(
        UUID orderId,
        String userName,
        String itemName,
        int quantity
) {
}
