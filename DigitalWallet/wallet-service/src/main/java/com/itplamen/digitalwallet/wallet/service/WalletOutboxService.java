package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.OutboxMessage;
import com.itplamen.digitalwallet.wallet.repository.OutboxMessageRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class WalletOutboxService {

    private final ObjectMapper objectMapper;
    private final OutboxMessageRepository outboxRepository;

    public Mono<Void> save(UUID paymentId, String eventType, Object eventPayload, Instant createdAt) {
        try {
            OutboxMessage outbox = OutboxMessage.builder()
                    .aggregateType("WALLET")
                    .aggregateId(paymentId.toString())
                    .eventType(eventType)
                    .payload(Json.of(objectMapper.writeValueAsString(eventPayload)))
                    .createdAt(createdAt)
                    .build();

            return outboxRepository.save(outbox).then();
        } catch (JacksonException e) {
            log.error("Failed to serialize outbox payload for payment ID: {}", paymentId, e);
            return Mono.error(e);
        }
    }
}
