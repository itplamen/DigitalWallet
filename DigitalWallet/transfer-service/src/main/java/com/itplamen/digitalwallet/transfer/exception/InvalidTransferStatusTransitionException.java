package com.itplamen.digitalwallet.transfer.exception;

import com.itplamen.digitalwallet.transfer.entity.TransferStatus;

import java.util.UUID;

public class InvalidTransferStatusTransitionException extends RuntimeException {
    public InvalidTransferStatusTransitionException(UUID transferId, TransferStatus from, TransferStatus to) {
        super("Invalid transfer status transition for " + transferId + ": " + from + " -> " + to);
    }
}
