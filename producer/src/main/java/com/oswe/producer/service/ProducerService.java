package com.oswe.producer.service;

import com.oswe.producer.messaging.event.OrderCreatedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final String topic;

    public ProducerService(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(OrderCreatedEvent event) {
        kafkaTemplate.send(topic, event.orderId().toString(), event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        System.err.println("Error sending the message " + error.getMessage());
                        return;
                    }

                    System.out.println("Message sent to the partition: "
                            + result.getRecordMetadata().partition());
                });
    }

}
