package com.itplamen.digitalwallet.transfer.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.transfer.service.TransferService;
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
public class TransferEventConsumer {

    private final ObjectMapper objectMapper;
    private final TransferService transferService;

    @KafkaListener(topics = "wallet-events", groupId = "transfer-service-group")
    public Mono<Void> consumeWalletEvent(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = eventType(record);
            return switch (eventType) {
                case "WalletTransferSettled" -> processWalletTransferSettled(record);
                case "WalletTransferNotSettled" -> processWalletTransferNotSettled(record);
                default -> {
                    log.debug("Ignoring wallet event type: {}", eventType);
                    yield Mono.empty();
                }
            };
        }).doOnError(error -> log.error("Failed to process wallet event from topic [{}], partition [{}]", record.topic(), record.partition(), error));
    }

    @KafkaListener(topics = "account-events", groupId = "transfer-service-group")
    public Mono<Void> consumeAccountEvent(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = eventType(record);
            if (!"AccountsRejected".equals(eventType)) {
                log.debug("Ignoring account event type: {}", eventType);
                return Mono.empty();
            }

            return processAccountsRejected(record);
        }).doOnError(error -> log.error("Failed to process account event from topic [{}], partition [{}]", record.topic(), record.partition(), error));
    }

    private String eventType(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("eventType");
        if (header == null) {
            throw new IllegalArgumentException("Missing eventType header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private Mono<Void> processWalletTransferSettled(ConsumerRecord<String, String> record) {
        if (record.value() == null || record.value().isBlank()) {
            return Mono.error(new IllegalArgumentException("Empty wallet event payload"));
        }

        UUID eventId = eventId(record);
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WalletEventPayload.class))
                .onErrorMap(JacksonException.class,
                        error -> new IllegalArgumentException("Invalid wallet event JSON", error))
                .flatMap(payload -> {
                    if (payload.transferId() == null) {
                        return Mono.error(new IllegalArgumentException("Missing transfer ID in wallet event"));
                    }
                    return transferService.processWalletTransferSettled(eventId, payload.transferId());
                });
    }

    private Mono<Void> processWalletTransferNotSettled(ConsumerRecord<String, String> record) {
        if (record.value() == null || record.value().isBlank()) {
            return Mono.error(new IllegalArgumentException("Empty WalletTransferNotSettled payload"));
        }

        UUID eventId = eventId(record);
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WalletEventPayload.class))
                .onErrorMap(JacksonException.class, error -> new IllegalArgumentException("Invalid WalletTransferNotSettled JSON", error))
                .flatMap(payload -> {
                    if (payload.transferId() == null || payload.reason() == null || payload.reason().isBlank()) {
                        return Mono.error(new IllegalArgumentException("WalletTransferNotSettled payload must include transferId and reason"));
                    }

                    return transferService.processWalletTransferNotSettled(eventId, payload.transferId(), payload.reason());
                });
    }

    private Mono<Void> processAccountsRejected(ConsumerRecord<String, String> record) {
        UUID eventId = eventId(record);
        if (record.value() == null || record.value().isBlank()) {
            return Mono.error(new IllegalArgumentException("Empty AccountsRejected payload"));
        }

        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), AccountsRejectedEvent.class))
                .onErrorMap(JacksonException.class, error -> new IllegalArgumentException("Invalid AccountsRejected JSON", error))
                .flatMap(payload -> {
                    if (payload.transferId() == null || payload.reason() == null || payload.reason().isBlank()) {
                        return Mono.error(new IllegalArgumentException("AccountsRejected payload must include transferId and reason"));
                    }

                    return transferService.processAccountsRejected(eventId, payload.transferId(), payload.reason());
                });
    }

    private UUID eventId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("eventId");
        if (header == null) {
            throw new IllegalArgumentException("Missing eventId header");
        }
        return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WalletEventPayload(UUID transferId, String reason) { }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AccountsRejectedEvent(UUID transferId, String reason) { }
}