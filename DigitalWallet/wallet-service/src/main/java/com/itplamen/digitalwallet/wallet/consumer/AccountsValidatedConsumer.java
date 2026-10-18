package com.itplamen.digitalwallet.wallet.consumer;

import com.itplamen.digitalwallet.wallet.service.WalletService;
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
public class AccountsValidatedConsumer {

    private static final String ACCOUNTS_VALIDATED = "AccountsValidated";

    private final ObjectMapper objectMapper;
    private final WalletService walletService;

    @KafkaListener(topics = "account-events", groupId = "wallet-service-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!ACCOUNTS_VALIDATED.equals(eventType)) {
                log.debug("Ignoring event type {} from account-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty AccountsValidated payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WalletService.TransferEventPayload.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid AccountsValidated JSON", error))
                    .flatMap(payload -> walletService.processAccountsValidated(eventId, payload));
        }).doOnSuccess(ignored -> log.info("Processed AccountsValidated event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process AccountsValidated event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
