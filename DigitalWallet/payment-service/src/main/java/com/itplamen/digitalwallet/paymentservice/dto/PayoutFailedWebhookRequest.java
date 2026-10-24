package com.itplamen.digitalwallet.paymentservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record PayoutFailedWebhookRequest(@NotNull UUID payoutId, @NotNull UUID paymentId, @NotBlank String reason) { }
