package com.itplamen.digitalwallet.paymentservice.consumer;

import com.itplamen.digitalwallet.paymentservice.event.WalletCreditedEvent;
import com.itplamen.digitalwallet.paymentservice.service.WalletCreditedConsumerService;
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
public class WalletCreditedConsumer {

    private static final String WALLET_CREDITED = "WalletCredited";

    private final ObjectMapper objectMapper;
    private final WalletCreditedConsumerService walletCreditedConsumerService;

    @KafkaListener(topics = "wallet-events", groupId = "payment-service-wallet-credited-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!WALLET_CREDITED.equals(eventType)) {
                log.debug("Ignoring event type {} from wallet-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty WalletCredited payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WalletCreditedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid WalletCredited JSON", error))
                    .flatMap(event -> {
                        if (event.paymentId() == null) {
                            log.debug("Ignoring WalletCredited without payment ID (P2P credit) with key {}", record.key());
                            return Mono.empty();
                        }

                        return walletCreditedConsumerService.handle(eventId, event);
                    });
        }).doOnSuccess(ignored -> log.info("Processed WalletCredited event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process WalletCredited event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
