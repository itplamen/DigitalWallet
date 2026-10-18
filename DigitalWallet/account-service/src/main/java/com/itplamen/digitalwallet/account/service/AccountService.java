package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.dto.AccountEligibilityResponse;
import com.itplamen.digitalwallet.account.dto.AccountEligibilityReason;
import com.itplamen.digitalwallet.account.entity.Account;
import com.itplamen.digitalwallet.account.entity.AccountStatus;
import com.itplamen.digitalwallet.account.exception.AccountAlreadyExistsException;
import com.itplamen.digitalwallet.account.exception.AccountNotFoundException;
import com.itplamen.digitalwallet.account.exception.InvalidAccountStatusTransitionException;
import com.itplamen.digitalwallet.account.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;

    public Mono<Account> create(String accountNumber, String customerName, String email) {
        Instant now = Instant.now();
        Account account = Account.builder()
                .id(UUID.randomUUID())
                .accountNumber(accountNumber.strip())
                .customerName(customerName)
                .email(email)
                .status(AccountStatus.ACTIVE)
                .kycChecked(false)
                .createdAt(now)
                .build();

        return accountRepository.save(account)
                .onErrorMap(DuplicateKeyException.class,
                        error -> new AccountAlreadyExistsException(account.getAccountNumber(), error));
    }

    public Mono<Account> get(UUID accountId) {
        return accountRepository.findById(accountId)
                .switchIfEmpty(Mono.error(new AccountNotFoundException(accountId)));
    }

    public Mono<Account> updateStatus(UUID accountId, AccountStatus requestedStatus) {
        return get(accountId)
                .flatMap(account -> {
                    if (account.getStatus() == requestedStatus) {
                        return Mono.just(account);
                    }

                    if (!isAllowedTransition(account.getStatus(), requestedStatus)) {
                        return Mono.error(new InvalidAccountStatusTransitionException(
                                accountId, account.getStatus(), requestedStatus));
                    }

                    account.setStatus(requestedStatus);
                    account.setUpdatedAt(Instant.now());

                    return accountRepository.save(account);
                });
    }

    public Mono<AccountEligibilityResponse> checkEligibility(UUID accountId) {
        return accountRepository.findById(accountId)
                .map(account -> account.getStatus() == AccountStatus.ACTIVE
                        ? new AccountEligibilityResponse(accountId, true, null)
                        : new AccountEligibilityResponse(accountId, false, account.getStatus().name()))
                .defaultIfEmpty(new AccountEligibilityResponse(
                        accountId, false, AccountEligibilityReason.NOT_FOUND.name()));
    }

    private Boolean isAllowedTransition(AccountStatus current, AccountStatus requested) {
        return switch (current) {
            case ACTIVE -> requested == AccountStatus.SUSPENDED || requested == AccountStatus.CLOSED;
            case SUSPENDED -> requested == AccountStatus.ACTIVE || requested == AccountStatus.CLOSED;
            case CLOSED -> false;
        };
    }
}
