package com.oswe.tracker.kafka;

import com.oswe.tracker.dto.OrderCreatedEvent;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class TrackerKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(TrackerKafkaConsumer.class);

    private final OrderCreatedEventHandler orderCreatedEventHandler;

    public TrackerKafkaConsumer(OrderCreatedEventHandler orderCreatedEventHandler) {
        this.orderCreatedEventHandler = orderCreatedEventHandler;
    }

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1_000, multiplier = 2),
            dltTopicSuffix = ".dlt"
    )
    @KafkaListener(topics = "${tracker.kafka.orders-topic}")
    public void handleOrders(OrderCreatedEvent orderCreatedEvent) {
        orderCreatedEventHandler.handle(orderCreatedEvent);
    }

    @DltHandler
    public void handleDeadLetter(ConsumerRecord<String, OrderCreatedEvent> record) {
        Header exceptionMessage = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);

        log.error(
                "Order event sent to DLT. topic={}, partition={}, offset={}, orderId={}, cause={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.value() == null ? null : record.value().orderId(),
                exceptionMessage == null ? "unknown" : new String(exceptionMessage.value(), StandardCharsets.UTF_8)
        );
    }
}
