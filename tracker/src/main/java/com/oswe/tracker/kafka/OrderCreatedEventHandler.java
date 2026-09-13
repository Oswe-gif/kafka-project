package com.oswe.tracker.kafka;

import com.oswe.tracker.dto.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderCreatedEventHandler {
    private static final Logger log = LoggerFactory.getLogger(OrderCreatedEventHandler.class);

    public void handle(OrderCreatedEvent event) {
        if ("sushi".equalsIgnoreCase(event.itemName())) {
            log.error("Log - Sushi orders are not supported");
            throw new IllegalStateException("Sushi orders are not supported");
        }

        log.info("Processing order-created event. orderId={}", event.orderId());
    }
}
