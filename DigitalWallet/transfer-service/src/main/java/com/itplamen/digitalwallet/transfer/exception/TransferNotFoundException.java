package com.itplamen.digitalwallet.transfer.exception;

public class TransferNotFoundException extends RuntimeException {
    public TransferNotFoundException(Long transferId) {
        super("Transfer not found: " + transferId);
    }
}
