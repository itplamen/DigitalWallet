package com.itplamen.digitalwallet.transfer.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
import com.itplamen.digitalwallet.transfer.repository.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransferEventConsumer {

    private final ObjectMapper objectMapper;
    private final TransferRepository transferRepository;

    @KafkaListener(topics = "wallet-events", groupId = "transfer-service-group")
    public Mono<Void> consumeWalletEvent(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            Header header = record.headers().lastHeader("eventType");

            if (header == null) {
                return Mono.error(new IllegalArgumentException("Missing eventType header"));
            }

            String eventType = new String(header.value(), StandardCharsets.UTF_8);

            log.info("Received wallet event [type: {}], topic [{}], partition [{}]", eventType, record.topic(), record.partition());

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
                            JsonProcessingException.class,
                            error -> new IllegalArgumentException("Invalid wallet event JSON", error)
                    )
                    .flatMap(payload -> {
                        if (payload.transferId() == null) {
                            return Mono.error(new IllegalArgumentException("Missing transfer ID in wallet event"));
                        }

                        return updateTransferStatus(payload.transferId(), targetStatus.get());
                    });
        }).doOnError(error ->
                log.error("Failed to process wallet event from topic [{}], partition [{}]", record.topic(), record.partition(), error));
    }

    private Mono<Void> updateTransferStatus(Long transferId, TransferStatus targetStatus) {
        return transferRepository.findById(transferId)
                .switchIfEmpty(Mono.error(new IllegalStateException("Transfer not found: " + transferId)))
                .flatMap(transfer -> {
                    TransferStatus current = transfer.getStatus();

                    if (current == targetStatus) {
                        log.info("Transfer {} already has status {}", transferId, current);
                        return Mono.empty();
                    }

                    if (!isAllowedTransition(current, targetStatus)) {
                        return Mono.error(
                                new IllegalStateException(
                                        "Invalid transfer status transition for "
                                                + transferId + ": "
                                                + current + " -> " + targetStatus
                                )
                        );
                    }

                    transfer.setStatus(targetStatus);

                    return transferRepository.save(transfer)
                            .doOnNext(saved -> log.info(
                                    "Updated transfer {} to status {}",
                                    saved.getId(), saved.getStatus()
                            ))
                            .then();
                });
    }

    private Optional<TransferStatus> mapEventToStatus(String eventType) {
        return switch (eventType) {
            case "WALLET_DEBITED" -> Optional.of(TransferStatus.SOURCE_DEBITED);
            case "WALLET_DEBIT_FAILED" -> Optional.of(TransferStatus.FAILED);
            case "TRANSFER_COMPLETED" -> Optional.of(TransferStatus.COMPLETED);
            case "TRANSFER_REVERSED" -> Optional.of(TransferStatus.REVERSED);
            default -> Optional.empty();
        };
    }

    private boolean isAllowedTransition(TransferStatus current, TransferStatus target) {
        if (current == null || isTerminal(current)) {
            return false;
        }

        if (current == TransferStatus.SOURCE_DEBITED) {
            return target == TransferStatus.COMPLETED
                    || target == TransferStatus.REVERSED;
        }

        return target == TransferStatus.SOURCE_DEBITED
                || target == TransferStatus.FAILED;
    }

    private boolean isTerminal(TransferStatus status) {
        return status == TransferStatus.COMPLETED
                || status == TransferStatus.FAILED
                || status == TransferStatus.REVERSED;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WalletEventPayload(@JsonProperty("id") Long transferId) { }
}