package com.itplamen.digitalwallet.paymentservice.consumer;

import com.itplamen.digitalwallet.paymentservice.event.PaymentAccountRejectedEvent;
import com.itplamen.digitalwallet.paymentservice.service.PaymentAccountRejectedConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentAccountRejectedConsumer {

    private static final String PAYMENT_ACCOUNT_REJECTED = "PaymentAccountRejected";

    private final ObjectMapper objectMapper;
    private final PaymentAccountRejectedConsumerService paymentAccountRejectedConsumerService;

    @KafkaListener(topics = "account-events", groupId = "payment-service-account-rejected-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!PAYMENT_ACCOUNT_REJECTED.equals(eventType)) {
                log.debug("Ignoring event type {} from account-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty PaymentAccountRejected payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), PaymentAccountRejectedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid PaymentAccountRejected JSON", error))
                    .flatMap(event -> {
                        if (event.paymentId() == null) {
                            return Mono.error(new IllegalArgumentException("PaymentAccountRejected payload has no payment ID"));
                        }

                        return paymentAccountRejectedConsumerService.handle(eventId, event);
                    });
        }).doOnSuccess(ignored -> log.info("Processed PaymentAccountRejected event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process PaymentAccountRejected event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
