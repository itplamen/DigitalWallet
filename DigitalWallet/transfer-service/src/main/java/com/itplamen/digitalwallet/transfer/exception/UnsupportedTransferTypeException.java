package com.itplamen.digitalwallet.transfer.exception;

import com.itplamen.digitalwallet.transfer.entity.TransferType;

public class UnsupportedTransferTypeException extends RuntimeException {
    public UnsupportedTransferTypeException(TransferType type) {
        super("Transfer type is not supported: " + type);
    }
}
