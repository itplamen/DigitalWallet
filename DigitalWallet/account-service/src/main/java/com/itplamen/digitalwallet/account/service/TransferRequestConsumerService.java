package com.itplamen.digitalwallet.account.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.account.entity.Account;
import com.itplamen.digitalwallet.account.entity.AccountStatus;
import com.itplamen.digitalwallet.account.repository.AccountRepository;
import com.itplamen.digitalwallet.account.repository.InboxMessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TransferRequestConsumerService {

    private static final String CONSUMER = "account-transfer-validator";

    private final AccountRepository accountRepository;
    private final InboxMessageRepository inboxMessageRepository;
    private final OutboxMessageService outboxMessageService;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> handle(UUID eventId, TransferRequestedEvent event) {
        return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                .flatMap(inserted -> {
                    if (inserted == 0) {
                        return Mono.empty();
                    }
                    return validateActiveAccounts(event.sourceAccountId(), event.targetAccountId())
                            .then(outboxMessageService.saveAccountsValidated(event));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> validateActiveAccounts(UUID sourceAccountId, UUID targetAccountId) {
        if (sourceAccountId == null || targetAccountId == null) {
            return Mono.error(new IllegalArgumentException("Transfer source and target account IDs are required"));
        }

        return accountRepository.findByIds(sourceAccountId, targetAccountId)
                .collectMap(Account::getId)
                .flatMap(accounts -> validateAccount(accounts, sourceAccountId, "source")
                        .then(validateAccount(accounts, targetAccountId, "target")));
    }

    private Mono<Void> validateAccount(Map<UUID, Account> accounts, UUID accountId, String role) {
        Account account = accounts.get(accountId);
        if (account == null) {
            return Mono.error(new IllegalStateException("Transfer " + role + " account does not exist: " + accountId));
        }

        if (account.getStatus() != AccountStatus.ACTIVE) {
            return Mono.error(new IllegalStateException("Transfer " + role + " account is not active: " + accountId));
        }

        return Mono.empty();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TransferRequestedEvent(
            UUID id,
            UUID sourceAccountId,
            UUID targetAccountId,
            BigDecimal amount,
            String currency) {
    }
}
