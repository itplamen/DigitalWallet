package com.itplamen.digitalwallet.account.dto;

import com.itplamen.digitalwallet.account.entity.AccountStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record AccountResponse(
        UUID id,
        String accountNumber,
        String customerName,
        String email,
        AccountStatus status,
        Boolean kycChecked,
        BigDecimal transferLimit,
        Instant createdAt,
        Instant updatedAt) {
}
