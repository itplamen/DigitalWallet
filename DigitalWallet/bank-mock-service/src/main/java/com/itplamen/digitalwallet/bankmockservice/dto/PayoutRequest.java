package com.itplamen.digitalwallet.bankmockservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record PayoutRequest(
        @NotNull UUID paymentId,
        @NotNull @Positive BigDecimal amount,
        @NotBlank String currency) { }
