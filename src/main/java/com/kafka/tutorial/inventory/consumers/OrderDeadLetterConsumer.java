package com.kafka.tutorial.inventory.consumers;

import com.kafka.tutorial.inventory.events.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderDeadLetterConsumer {
    @KafkaListener(topics = "orders-dlt")
    public void consume(OrderCreatedEvent orderCreatedEvent,
                        @Header(KafkaHeaders.DLT_ORIGINAL_TOPIC) String originalTopic,
                        @Header(KafkaHeaders.DLT_ORIGINAL_PARTITION) int originalPartition,
                        @Header(KafkaHeaders.DLT_ORIGINAL_OFFSET) long originalOffset,
                        @Header(KafkaHeaders.DLT_EXCEPTION_FQCN) String exceptionClass,
                        @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error("""
            Failed Event
            Original Topic      : {}
            Original Partition  : {}
            Original Offset      : {}
            Original Exception  : {}
            Original Exception message  : {}
            Event       : {}
            """,
                originalTopic,
                originalPartition,
                originalOffset,
                exceptionClass,
                exceptionMessage,
                orderCreatedEvent);
    }
}
