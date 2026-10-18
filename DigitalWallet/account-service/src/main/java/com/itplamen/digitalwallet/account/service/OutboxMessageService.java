package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.entity.OutboxMessage;
import com.itplamen.digitalwallet.account.repository.OutboxMessageRepository;
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
public class OutboxMessageService {

    private final ObjectMapper objectMapper;
    private final OutboxMessageRepository outboxMessageRepository;

    public Mono<Void> saveAccountsValidated(TransferRequestConsumerService.TransferRequestedEvent event) {
        return saveEvent(event.id(), "AccountsValidated", event);
    }

    public Mono<Void> saveAccountsRejected(TransferRequestConsumerService.AccountsRejectedEvent event) {
        return saveEvent(event.transferId(), "AccountsRejected", event);
    }

    private Mono<Void> saveEvent(UUID transferId, String eventType, Object payload) {
        try {
            OutboxMessage message = OutboxMessage.builder()
                    .aggregateType("Transfer")
                    .aggregateId(transferId.toString())
                    .eventType(eventType)
                    .payload(Json.of(objectMapper.writeValueAsString(payload)))
                    .createdAt(Instant.now())
                    .build();

            return outboxMessageRepository.save(message).then();
        } catch (JacksonException error) {
            return Mono.error(new IllegalStateException("Failed to serialize " + eventType + " event for transfer " + transferId, error));
        }
    }
}
