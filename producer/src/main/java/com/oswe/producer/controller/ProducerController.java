package com.oswe.producer.controller;

import com.oswe.producer.controller.dto.CreateOrderRequest;
import com.oswe.producer.messaging.event.OrderCreatedEvent;
import com.oswe.producer.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/messages")
public class ProducerController {
    private final ProducerService producerService;

    @PostMapping
    public ResponseEntity<Void> send(@RequestBody CreateOrderRequest request) {
        var event = new OrderCreatedEvent(
                request.orderId() != null ? request.orderId() : UUID.randomUUID(),
                request.userName(),
                request.itemName(),
                request.quantity()
        );

        producerService.send(event);
        return ResponseEntity.accepted().build();
    }

}
