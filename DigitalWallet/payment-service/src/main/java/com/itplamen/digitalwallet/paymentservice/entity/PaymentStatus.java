package com.itplamen.digitalwallet.paymentservice.entity;

public enum PaymentStatus {
    PENDING,
    WALLET_VALIDATED,
    CHARGED,
    FUNDS_RESERVED,
    COMPLETED,
    FAILED,
    REFUNDED
}
