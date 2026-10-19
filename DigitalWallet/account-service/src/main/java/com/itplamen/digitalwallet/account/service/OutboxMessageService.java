package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.entity.OutboxMessage;
import com.itplamen.digitalwallet.account.entity.Account;
import com.itplamen.digitalwallet.account.event.AccountsRejectedEvent;
import com.itplamen.digitalwallet.account.event.TransferRequestedEvent;
import com.itplamen.digitalwallet.account.event.WithdrawalRequestedEvent;
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

    public Mono<Void> saveAccountsValidated(TransferRequestedEvent event) {
        return saveEvent(event.id(), "AccountsValidated", event);
    }

    public Mono<Void> saveAccountsRejected(AccountsRejectedEvent event) {
        return saveEvent(event.transferId(), "AccountsRejected", event);
    }

    public Mono<Void> saveWithdrawalAccountValidated(WithdrawalRequestedEvent event) {
        return saveEvent(
                event.id(),
                "WithdrawalAccountValidated",
                new WithdrawalAccountValidatedEvent(event.id(), event.accountId(), event.amount()));
    }

    public Mono<Void> saveAccountCreated(Account account) {
        return saveEvent(
                account.getId(),
                "AccountCreated",
                new AccountCreatedEvent(account.getId(), account.getEmail(), account.getCustomerName()),
                "Account");
    }

    private Mono<Void> saveEvent(UUID transferId, String eventType, Object payload) {
        return saveEvent(transferId, eventType, payload, "Transfer");
    }

    private Mono<Void> saveEvent(UUID aggregateId, String eventType, Object payload, String aggregateType) {
        try {
            OutboxMessage message = OutboxMessage.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId.toString())
                    .eventType(eventType)
                    .payload(Json.of(objectMapper.writeValueAsString(payload)))
                    .createdAt(Instant.now())
                    .build();

            return outboxMessageRepository.save(message).then();
        } catch (JacksonException error) {
            return Mono.error(new IllegalStateException(
                    "Failed to serialize " + eventType + " event for aggregate " + aggregateId, error));
        }
    }

    public record AccountCreatedEvent(UUID accountId, String email, String name) { }

    public record WithdrawalAccountValidatedEvent(UUID withdrawalId, UUID accountId, java.math.BigDecimal amount) { }
}
