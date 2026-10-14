package com.itplamen.digitalwallet.wallet.consumer;

import com.itplamen.digitalwallet.wallet.service.WalletService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransferEventConsumer {

    private final ObjectMapper objectMapper;
    private final WalletService walletService;

    @KafkaListener(topics = "transfer-events", groupId = "wallet-service-group")
    public Mono<Void> consumeTransferEvent(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
                    log.info("Received transfer event from topic [{}], partition [{}], key [{}]", record.topic(), record.partition(), record.key());

                    if (record.value() == null || record.value().isBlank()) {
                        return Mono.error(new IllegalArgumentException("Empty transfer event payload"));
                    }

                    return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WalletService.TransferEventPayload.class))
                            .onErrorMap(
                                    JacksonException.class,
                                    error -> new IllegalArgumentException("Invalid transfer event JSON", error)
                            )
                            .flatMap(walletService::processTransfer);
                }).doOnSuccess(v -> log.info("Successfully processed transfer debit event from partition [{}]", record.partition()))
                .doOnError(error -> log.error("Failed to process transfer event from topic [{}], partition [{}]",
                        record.topic(), record.partition(), error));
    }
}