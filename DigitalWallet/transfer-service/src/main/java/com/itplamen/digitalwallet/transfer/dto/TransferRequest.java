package com.itplamen.digitalwallet.transfer.dto;

import com.itplamen.digitalwallet.transfer.entity.TransferType;
import java.math.BigDecimal;

public record TransferRequest(
        Long sourceWalletId,
        Long targetWalletId,
        BigDecimal amount,
        String currency,
        TransferType type,
        String idempotencyKey
) {}