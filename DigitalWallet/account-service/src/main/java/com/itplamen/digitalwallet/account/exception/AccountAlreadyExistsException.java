package com.itplamen.digitalwallet.account.exception;

public class AccountAlreadyExistsException extends RuntimeException {

    public AccountAlreadyExistsException(String accountNumber, Throwable cause) {
        super("Account number already exists: " + accountNumber, cause);
    }
}
