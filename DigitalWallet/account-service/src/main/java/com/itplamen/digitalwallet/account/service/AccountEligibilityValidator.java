package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.entity.Account;
import com.itplamen.digitalwallet.account.entity.AccountStatus;
import com.itplamen.digitalwallet.account.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class AccountEligibilityValidator {

    private final AccountRepository accountRepository;

    public Mono<ValidationResult> validateTransferAccounts(UUID sourceAccountId, UUID targetAccountId) {
        if (sourceAccountId == null || targetAccountId == null) {
            return Mono.error(new IllegalArgumentException("Transfer source and target account IDs are required"));
        }

        return accountRepository.findByIds(sourceAccountId, targetAccountId)
                .collectMap(Account::getId)
                .map(accounts -> validateTransferAccounts(accounts, sourceAccountId, targetAccountId));
    }

    public Mono<ValidationResult> validateWithdrawalAccount(UUID accountId, BigDecimal amount) {
        if (accountId == null) {
            return Mono.just(ValidationResult.rejected("ACCOUNT_ID_REQUIRED"));
        }
        if (amount == null || amount.signum() <= 0) {
            return Mono.just(ValidationResult.rejected("WITHDRAWAL_AMOUNT_INVALID"));
        }

        return accountRepository.findById(accountId)
                .map(account -> {
                    String reason = accountEligibilityFailure(account, "ACCOUNT");
                    if (reason != null) {
                        return ValidationResult.rejected(reason);
                    }
                    if (account.getTransferLimit() == null || amount.compareTo(account.getTransferLimit()) > 0) {
                        return ValidationResult.rejected("WITHDRAWAL_LIMIT_EXCEEDED");
                    }
                    return ValidationResult.accepted();
                })
                .defaultIfEmpty(ValidationResult.rejected("ACCOUNT_NOT_FOUND"));
    }

    private ValidationResult validateTransferAccounts(
            Map<UUID, Account> accounts,
            UUID sourceAccountId,
            UUID targetAccountId) {
        String sourceFailure = accountEligibilityFailure(accounts.get(sourceAccountId), "SOURCE");
        if (sourceFailure != null) {
            return ValidationResult.rejected(sourceFailure);
        }

        String targetFailure = accountEligibilityFailure(accounts.get(targetAccountId), "TARGET");
        return targetFailure == null
                ? ValidationResult.accepted()
                : ValidationResult.rejected(targetFailure);
    }

    private String accountEligibilityFailure(Account account, String role) {
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

    public record ValidationResult(boolean valid, String reason) {
        private static ValidationResult accepted() {
            return new ValidationResult(true, null);
        }

        private static ValidationResult rejected(String reason) {
            return new ValidationResult(false, reason);
        }
    }
}
