package com.itplamen.digitalwallet.transfer.service;

import com.itplamen.digitalwallet.transfer.entity.OutboxMessage;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.repository.OutboxMessageRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TransferEventOutboxService {

    private final ObjectMapper objectMapper;
    private final OutboxMessageRepository outboxMessageRepository;

    public Mono<Void> saveInitiated(Transfer transfer) {
        OutboxMessage outboxMessage = buildInitiatedEvent(transfer);
        return outboxMessageRepository.save(outboxMessage).then();
    }

    public Mono<Void> saveCompleted(Transfer transfer) {
        try {
            String payload = objectMapper.writeValueAsString(new TransferCompletedEvent(transfer.getId()));
            OutboxMessage outboxMessage = OutboxMessage.builder()
                    .aggregateType("Transfer")
                    .aggregateId(transfer.getId().toString())
                    .eventType("TransferCompleted")
                    .payload(Json.of(payload))
                    .createdAt(Instant.now())
                    .build();
            return outboxMessageRepository.save(outboxMessage).then();
        } catch (JacksonException error) {
            return Mono.error(new IllegalStateException("Failed to serialize TransferCompleted event for transfer " + transfer.getId(), error));
        }
    }

    private OutboxMessage buildInitiatedEvent(Transfer transfer) {
        try {
            String payload = objectMapper.writeValueAsString(transfer);

            return OutboxMessage.builder()
                    .aggregateType("Transfer")
                    .aggregateId(String.valueOf(transfer.getId()))
                    .eventType("TransferRequested")
                    .payload(Json.of(payload))
                    .createdAt(Instant.now())
                    .build();
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize transfer to outbox payload", e);
        }
    }

    public record TransferCompletedEvent(UUID transferId) { }
}
