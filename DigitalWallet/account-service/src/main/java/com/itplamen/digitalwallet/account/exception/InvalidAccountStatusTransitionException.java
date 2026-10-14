package com.itplamen.digitalwallet.account.exception;

import com.itplamen.digitalwallet.account.entity.AccountStatus;

public class InvalidAccountStatusTransitionException extends RuntimeException {

    public InvalidAccountStatusTransitionException(
            Long accountId,
            AccountStatus currentStatus,
            AccountStatus requestedStatus) {
        super("Cannot change account " + accountId + " from " + currentStatus + " to " + requestedStatus);
    }
}
