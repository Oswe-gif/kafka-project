package com.oswe.tracker.kafka;

import com.oswe.tracker.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TrackerKafkaConsumer {
    @KafkaListener(topics = "orders")
    public void handleOrders(OrderCreatedEvent orderCreatedEvent) {
        try {
            System.out.println("message received: " + orderCreatedEvent.toString());

        } catch (Exception e) {
            throw new RuntimeException("sorry buddy", e);
        }
    }
}
