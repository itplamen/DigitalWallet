package com.itplamen.digitalwallet.transfer.service;

import com.itplamen.digitalwallet.transfer.entity.OutboxMessage;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class TransferEventOutboxService {

    private final ObjectMapper objectMapper;
    private final OutboxMessageRepository outboxMessageRepository;

    public Mono<Void> saveInitiated(Transfer transfer) {
        OutboxMessage outboxMessage = buildInitiatedEvent(transfer);
        return outboxMessageRepository.save(outboxMessage).then();
    }

    private OutboxMessage buildInitiatedEvent(Transfer transfer) {
        try {
            String payload = objectMapper.writeValueAsString(transfer);

            return OutboxMessage.builder()
                    .aggregateType("Transfer")
                    .aggregateId(String.valueOf(transfer.getId()))
                    .eventType("TRANSFER_INITIATED")
                    .payload(payload)
                    .processed(false)
                    .createdAt(Instant.now())
                    .build();
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize transfer to outbox payload", e);
        }
    }
}
