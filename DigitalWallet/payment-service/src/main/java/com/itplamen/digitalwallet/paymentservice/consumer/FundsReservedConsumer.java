package com.itplamen.digitalwallet.paymentservice.consumer;

import com.itplamen.digitalwallet.paymentservice.event.FundsReservedEvent;
import com.itplamen.digitalwallet.paymentservice.service.FundsReservedConsumerService;
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
public class FundsReservedConsumer {

    private static final String FUNDS_RESERVED = "FundsReserved";

    private final ObjectMapper objectMapper;
    private final FundsReservedConsumerService fundsReservedConsumerService;

    @KafkaListener(topics = "wallet-events", groupId = "payment-service-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!FUNDS_RESERVED.equals(eventType)) {
                log.debug("Ignoring event type {} from wallet-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty FundsReserved payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), FundsReservedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid FundsReserved JSON", error))
                    .flatMap(event -> {
                        if (event.paymentId() == null) {
                            return Mono.error(new IllegalArgumentException("FundsReserved payload has no payment ID"));
                        }

                        return fundsReservedConsumerService.handle(eventId, event);
                    });
        }).doOnSuccess(ignored -> log.info("Processed FundsReserved event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process FundsReserved event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
