package com.itplamen.digitalwallet.account.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.account.entity.Account;
import com.itplamen.digitalwallet.account.entity.AccountStatus;
import com.itplamen.digitalwallet.account.repository.AccountRepository;
import com.itplamen.digitalwallet.account.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.account.event.WithdrawalRequestedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WithdrawalRequestConsumerService {

    private static final String CONSUMER = "account-withdrawal-validator";

    private final AccountRepository accountRepository;
    private final InboxMessageRepository inboxMessageRepository;
    private final OutboxMessageService outboxMessageService;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> handle(UUID eventId, WithdrawalRequestedEvent event) {
        return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                .flatMap(inserted -> {
                    if (inserted == 0) {
                        return Mono.empty();
                    }

                    return validate(event)
                            .flatMap(valid -> valid
                                    ? outboxMessageService.saveWithdrawalAccountValidated(event)
                                    : Mono.empty());
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Boolean> validate(WithdrawalRequestedEvent event) {
        if (event.accountId() == null) {
            return Mono.just(false);
        }
        if (event.amount() == null || event.amount().signum() <= 0) {
            return Mono.just(false);
        }

        return accountRepository.findById(event.accountId())
                .map(account -> isAccountValid(account, event.amount()))
                .defaultIfEmpty(false);
    }

    private boolean isAccountValid(Account account, BigDecimal amount) {
        return account.getStatus() == AccountStatus.ACTIVE
                && Boolean.TRUE.equals(account.getKycChecked())
                && account.getTransferLimit() != null
                && amount.compareTo(account.getTransferLimit()) <= 0;
    }
}
