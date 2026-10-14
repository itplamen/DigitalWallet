package com.itplamen.digitalwallet.transfer.dto;

import com.itplamen.digitalwallet.transfer.entity.TransferType;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record TransferRequest(
        @NotNull(message = "Source wallet ID is required")
        @Positive(message = "Source wallet ID must be positive")
        Long sourceWalletId,

        @NotNull(message = "Target wallet ID is required")
        @Positive(message = "Target wallet ID must be positive")
        Long targetWalletId,

        @NotNull(message = "Amount is required")
        @Positive(message = "Amount must be positive")
        @Digits(integer = 11, fraction = 8, message = "Amount must have at most 11 integer digits and 8 decimal places")
        BigDecimal amount,

        @NotBlank(message = "Currency is required")
        @Pattern(regexp = "[A-Za-z]{3}", message = "Currency must be a 3-letter code")
        String currency,

        @NotNull(message = "Transfer type is required")
        TransferType type,

        @NotBlank(message = "Idempotency key is required")
        @Size(max = 128, message = "Idempotency key must not exceed 128 characters")
        String idempotencyKey
) {
    @AssertTrue(message = "Source and target wallets must be different")
        public boolean isWalletPairValid() {
        return sourceWalletId == null || targetWalletId == null || !sourceWalletId.equals(targetWalletId);
    }
}