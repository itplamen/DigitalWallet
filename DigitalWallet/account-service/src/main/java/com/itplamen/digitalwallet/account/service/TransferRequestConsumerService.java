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
                    return validateActiveAccount(event.sourceAccountId(), "source")
                            .then(validateActiveAccount(event.targetAccountId(), "target"))
                            .then(outboxMessageService.saveAccountsValidated(event));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Account> validateActiveAccount(UUID accountId, String role) {
        if (accountId == null) {
            return Mono.error(new IllegalArgumentException("Transfer " + role + " account ID is missing"));
        }

        return accountRepository.findById(accountId)
                .switchIfEmpty(Mono.error(new IllegalStateException("Transfer " + role + " account does not exist: " + accountId)))
                .flatMap(account -> account.getStatus() == AccountStatus.ACTIVE
                        ? Mono.just(account)
                        : Mono.error(new IllegalStateException("Transfer " + role + " account is not active: " + accountId)));
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
