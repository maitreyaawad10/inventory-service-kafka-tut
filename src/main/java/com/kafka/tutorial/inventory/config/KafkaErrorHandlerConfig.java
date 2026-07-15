package com.kafka.tutorial.inventory.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // the consumer temporarily becomes a producer to send te failed message to the DLT
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        // retry attemps = 3, each after a gap of 5 seconds - total attempts = 4(3 retries + 1 original)
        FixedBackOff fixedBackOff = new FixedBackOff(5000L, 3);

        // for handling different exceptions/errors
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer, fixedBackOff);

        // for sending some messages directly to DLT instead of retrying them
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class
        );

        // return the errorHandled bean
        return errorHandler;
    }
}
