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

@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;

    public Mono<Account> create(String accountNumber) {
        Account account = Account.builder()
                .accountNumber(accountNumber.strip())
                .status(AccountStatus.ACTIVE)
                .createdAt(Instant.now())
                .build();

        return accountRepository.save(account)
                .onErrorMap(DuplicateKeyException.class,
                        error -> new AccountAlreadyExistsException(account.getAccountNumber(), error));
    }

    public Mono<Account> get(Long accountId) {
        return accountRepository.findById(accountId)
                .switchIfEmpty(Mono.error(new AccountNotFoundException(accountId)));
    }

    public  Mono<Account> get(String accountNumber) {
        return  accountRepository.findByAccountNumber(accountNumber)
                .switchIfEmpty(Mono.error(new AccountNotFoundException(accountNumber)));
    }

    public Mono<Account> updateStatus(Long accountId, AccountStatus requestedStatus) {
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

                    return accountRepository.save(account);
                });
    }

    public Mono<AccountEligibilityResponse> checkEligibility(Long accountId) {
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
