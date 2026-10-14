package com.itplamen.digitalwallet.account.dto;

import com.itplamen.digitalwallet.account.entity.AccountStatus;

import java.time.Instant;

public record AccountResponse(
        Long id,
        String accountNumber,
        AccountStatus status,
        Instant createdAt) {
}
