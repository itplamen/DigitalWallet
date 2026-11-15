package com.itplamen.digitalwallet.wallet.consumer;

import com.itplamen.digitalwallet.wallet.event.DepositConfirmedEvent;
import com.itplamen.digitalwallet.wallet.service.DepositCreditService;
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
public class DepositConfirmedConsumer {

    private static final String DEPOSIT_CONFIRMED = "DepositConfirmed";

    private final ObjectMapper objectMapper;
    private final DepositCreditService depositCreditService;

    @KafkaListener(topics = "payment-events", groupId = "wallet-service-deposit-confirmed-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!DEPOSIT_CONFIRMED.equals(eventType)) {
                log.debug("Ignoring event type {} from payment-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty DepositConfirmed payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), DepositConfirmedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid DepositConfirmed JSON", error))
                    .flatMap(event -> depositCreditService.creditWallet(eventId, event));
        }).doOnSuccess(ignored -> log.info("Processed DepositConfirmed event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process DepositConfirmed event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
