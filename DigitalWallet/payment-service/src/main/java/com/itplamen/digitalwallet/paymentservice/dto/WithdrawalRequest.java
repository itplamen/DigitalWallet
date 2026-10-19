package com.itplamen.digitalwallet.paymentservice.dto;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record WithdrawalRequest(
        @NotNull(message = "Account ID is required")
        UUID accountId,

        @NotNull(message = "Amount is required")
        @Positive(message = "Amount must be positive")
        @Digits(integer = 11, fraction = 8, message = "Amount must have at most 11 integer digits and 8 decimal places")
        BigDecimal amount,

        @NotBlank(message = "Currency is required")
        @Pattern(regexp = "[A-Za-z]{3}", message = "Currency must be a 3-letter code")
        String currency) { }
