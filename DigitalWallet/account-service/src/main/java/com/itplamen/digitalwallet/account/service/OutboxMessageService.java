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

@Service
@RequiredArgsConstructor
public class OutboxMessageService {

    private final ObjectMapper objectMapper;
    private final OutboxMessageRepository outboxMessageRepository;

    public Mono<Void> saveAccountsValidated(TransferRequestConsumerService.TransferRequestedEvent event) {
        try {
            OutboxMessage message = OutboxMessage.builder()
                    .aggregateType("Transfer")
                    .aggregateId(event.id().toString())
                    .eventType("AccountsValidated")
                    .payload(Json.of(objectMapper.writeValueAsString(event)))
                    .createdAt(Instant.now())
                    .build();

            return outboxMessageRepository.save(message).then();
        } catch (JacksonException error) {
            return Mono.error(new IllegalStateException("Failed to serialize AccountsValidated event for transfer " + event.id(), error));
        }
    }
}
