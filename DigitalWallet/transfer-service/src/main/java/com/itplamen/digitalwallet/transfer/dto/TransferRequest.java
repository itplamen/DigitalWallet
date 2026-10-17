package com.itplamen.digitalwallet.transfer.dto;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;
import java.util.UUID;

public record TransferRequest(
        @NotNull(message = "Source account ID is required")
        UUID sourceAccountId,

        @NotNull(message = "Target account ID is required")
        UUID targetAccountId,

        @NotNull(message = "Amount is required")
        @Positive(message = "Amount must be positive")
        @Digits(integer = 11, fraction = 8, message = "Amount must have at most 11 integer digits and 8 decimal places")
        BigDecimal amount,

        @NotBlank(message = "Currency is required")
        @Pattern(regexp = "[A-Za-z]{3}", message = "Currency must be a 3-letter code")
        String currency
) {
    @AssertTrue(message = "Source and target accounts must be different")
    public boolean isAccountPairValid() {
        return sourceAccountId == null || targetAccountId == null || !sourceAccountId.equals(targetAccountId);
    }
}