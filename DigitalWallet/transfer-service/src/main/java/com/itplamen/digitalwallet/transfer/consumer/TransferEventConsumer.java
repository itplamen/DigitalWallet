package com.itplamen.digitalwallet.transfer.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
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
import java.util.Optional;
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
            Header header = record.headers().lastHeader("eventType");

            if (header == null) {
                return Mono.error(new IllegalArgumentException("Missing eventType header"));
            }

            String eventType = new String(header.value(), StandardCharsets.UTF_8);

            log.info("Received wallet event [type: {}], topic [{}], partition [{}]", eventType, record.topic(), record.partition());

            if ("WalletTransferSettled".equals(eventType)) {
                return processWalletTransferSettled(record);
            }

            Optional<TransferStatus> targetStatus = mapEventToStatus(eventType);

            if (targetStatus.isEmpty()) {
                log.warn("Ignoring unknown event type: {}", eventType);
                return Mono.empty();
            }

            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty wallet event payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WalletEventPayload.class))
                    .onErrorMap(
                            JacksonException.class,
                            error -> new IllegalArgumentException("Invalid wallet event JSON", error)
                    )
                    .flatMap(payload -> {
                        if (payload.transferId() == null) {
                            return Mono.error(new IllegalArgumentException("Missing transfer ID in wallet event"));
                        }

                        return transferService.transitionStatus(
                                payload.transferId(),
                                targetStatus.get(),
                                payload.reason() == null ? eventType : payload.reason())
                                .then();
                    });
        }).doOnError(error ->
                log.error("Failed to process wallet event from topic [{}], partition [{}]", record.topic(), record.partition(), error));
    }

    private Mono<Void> processWalletTransferSettled(ConsumerRecord<String, String> record) {
        Header eventIdHeader = record.headers().lastHeader("eventId");
        if (eventIdHeader == null) {
            return Mono.error(new IllegalArgumentException("Missing eventId header"));
        }
        if (record.value() == null || record.value().isBlank()) {
            return Mono.error(new IllegalArgumentException("Empty wallet event payload"));
        }

        UUID eventId = UUID.fromString(new String(eventIdHeader.value(), StandardCharsets.UTF_8));
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

    private Optional<TransferStatus> mapEventToStatus(String eventType) {
        return switch (eventType) {
            case "WALLET_DEBITED" -> Optional.of(TransferStatus.PENDING);
            case "WALLET_DEBIT_FAILED" -> Optional.of(TransferStatus.FAILED);
            case "WALLET_TRANSFER_FAILED" -> Optional.of(TransferStatus.FAILED);
            case "TRANSFER_COMPLETED" -> Optional.of(TransferStatus.COMPLETED);
            default -> Optional.empty();
        };
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WalletEventPayload(UUID transferId, String reason) { }
}