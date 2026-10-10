package com.itplamen.digitalwallet.transfer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransferEventConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "transfer-events", groupId = "transfer-service-group")
    public void consumeTransferEvent(ConsumerRecord<String, String> record) {
        try {
            Transfer transfer = objectMapper.readValue(record.value(), Transfer.class);

            log.info("Successfully consumed and deserialized Transfer event:");
            log.info(" - Transfer ID: {}", transfer.getId());
            log.info(" - Source Wallet: {}", transfer.getSourceWalletId());
            log.info(" - Target Wallet: {}", transfer.getTargetWalletId());
            log.info(" - Amount: {} {}", transfer.getAmount(), transfer.getCurrency());
            log.info(" - Status: {}", transfer.getStatus());
            log.info("--------------------------------------------------");
        } catch (Exception e) {
            log.error("Failed to deserialize Kafka event payload: {}", record.value(), e);
        }
    }
}