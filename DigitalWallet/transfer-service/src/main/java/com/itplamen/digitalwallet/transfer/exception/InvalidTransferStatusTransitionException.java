package com.itplamen.digitalwallet.transfer.exception;

import com.itplamen.digitalwallet.transfer.entity.TransferStatus;

public class InvalidTransferStatusTransitionException extends RuntimeException {
    public InvalidTransferStatusTransitionException(Long transferId, TransferStatus from, TransferStatus to) {
        super("Invalid transfer status transition for " + transferId + ": " + from + " -> " + to);
    }
}
