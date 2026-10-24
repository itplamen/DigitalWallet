package com.itplamen.digitalwallet.paymentservice.consumer;

import com.itplamen.digitalwallet.paymentservice.event.FundsReleasedEvent;
import com.itplamen.digitalwallet.paymentservice.service.FundsReleasedConsumerService;
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
public class FundsReleasedConsumer {

    private static final String FUNDS_RELEASED = "FundsReleased";

    private final ObjectMapper objectMapper;
    private final FundsReleasedConsumerService fundsReleasedConsumerService;

    @KafkaListener(topics = "wallet-events", groupId = "payment-service-funds-released-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!FUNDS_RELEASED.equals(eventType)) {
                log.debug("Ignoring event type {} from wallet-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty FundsReleased payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), FundsReleasedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid FundsReleased JSON", error))
                    .flatMap(event -> {
                        if (event.paymentId() == null) {
                            return Mono.error(new IllegalArgumentException("FundsReleased payload has no payment ID"));
                        }

                        return fundsReleasedConsumerService.handle(eventId, event);
                    });
        }).doOnSuccess(ignored -> log.info("Processed FundsReleased event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process FundsReleased event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
