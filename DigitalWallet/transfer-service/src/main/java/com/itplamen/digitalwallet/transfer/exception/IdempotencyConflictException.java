package com.itplamen.digitalwallet.transfer.exception;

public class IdempotencyConflictException extends RuntimeException {
    public IdempotencyConflictException(String idempotencyKey) {
        super("Idempotency key has already been used for a different transfer: " + idempotencyKey);
    }
}
