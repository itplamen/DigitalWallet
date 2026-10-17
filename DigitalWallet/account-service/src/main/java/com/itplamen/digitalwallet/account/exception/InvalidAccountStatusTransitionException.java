package com.itplamen.digitalwallet.account.exception;

import com.itplamen.digitalwallet.account.entity.AccountStatus;

import java.util.UUID;

public class InvalidAccountStatusTransitionException extends RuntimeException {

    public InvalidAccountStatusTransitionException(
            UUID accountId,
            AccountStatus currentStatus,
            AccountStatus requestedStatus) {
        super("Cannot change account " + accountId + " from " + currentStatus + " to " + requestedStatus);
    }
}
