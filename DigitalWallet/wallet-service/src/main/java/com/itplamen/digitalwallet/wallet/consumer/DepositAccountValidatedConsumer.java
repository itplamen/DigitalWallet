package com.itplamen.digitalwallet.wallet.consumer;

import com.itplamen.digitalwallet.wallet.event.PaymentAccountValidatedEvent;
import com.itplamen.digitalwallet.wallet.service.DepositWalletValidationService;
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
public class DepositAccountValidatedConsumer {

    private static final String DEPOSIT_ACCOUNT_VALIDATED = "DepositAccountValidated";

    private final ObjectMapper objectMapper;
    private final DepositWalletValidationService depositWalletValidationService;

    @KafkaListener(topics = "account-events", groupId = "wallet-service-deposit-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!DEPOSIT_ACCOUNT_VALIDATED.equals(eventType)) {
                log.debug("Ignoring event type {} from account-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty DepositAccountValidated payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(
                            record.value(), PaymentAccountValidatedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid DepositAccountValidated JSON", error))
                    .flatMap(event -> depositWalletValidationService.validateWallet(eventId, event));
        }).doOnSuccess(ignored -> log.info("Processed DepositAccountValidated event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process DepositAccountValidated event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
