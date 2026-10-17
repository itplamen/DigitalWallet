package com.itplamen.digitalwallet.account.exception;

import java.util.UUID;

public class AccountNotFoundException extends RuntimeException {

    public AccountNotFoundException(UUID accountId) {
        super("Account not found: " + accountId);
    }

    public AccountNotFoundException(String accountNumber) {
        super("Account not found: " + accountNumber);
    }
}
