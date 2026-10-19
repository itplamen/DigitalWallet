package com.itplamen.digitalwallet.account.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.account.entity.Account;
import com.itplamen.digitalwallet.account.entity.AccountStatus;
import com.itplamen.digitalwallet.account.repository.AccountRepository;
import com.itplamen.digitalwallet.account.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.account.event.AccountsRejectedEvent;
import com.itplamen.digitalwallet.account.event.TransferRequestedEvent;
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
                            .flatMap(validation -> validation.valid()
                                    ? outboxMessageService.saveAccountsValidated(event)
                                    : outboxMessageService.saveAccountsRejected(new AccountsRejectedEvent(event.id(), validation.reason())));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<AccountValidation> validateActiveAccounts(UUID sourceAccountId, UUID targetAccountId) {
        if (sourceAccountId == null || targetAccountId == null) {
            return Mono.error(new IllegalArgumentException("Transfer source and target account IDs are required"));
        }

        return accountRepository.findByIds(sourceAccountId, targetAccountId)
                .collectMap(Account::getId)
                .map(accounts -> {
                    String sourceReason = accountRejectionReason(accounts.get(sourceAccountId), "SOURCE");
                    if (sourceReason != null) {
                        return new AccountValidation(false, sourceReason);
                    }
                    String targetReason = accountRejectionReason(accounts.get(targetAccountId), "TARGET");

                    return targetReason == null
                            ? new AccountValidation(true, null)
                            : new AccountValidation(false, targetReason);
                });
    }

    private String accountRejectionReason(Account account, String role) {
        if (account == null) {
            return role + "_ACCOUNT_NOT_FOUND";
        }

        return switch (account.getStatus()) {
            case ACTIVE -> Boolean.TRUE.equals(account.getKycChecked())
                    ? null
                    : role + "_KYC_NOT_CHECKED";
            case SUSPENDED -> role + "_ACCOUNT_SUSPENDED";
            case CLOSED -> role + "_ACCOUNT_CLOSED";
        };
    }

    private record AccountValidation(boolean valid, String reason) { }

}
