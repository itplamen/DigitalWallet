package com.itplamen.digitalwallet.paymentservice.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record PayoutSucceededWebhookRequest(@NotNull UUID payoutId, @NotNull UUID paymentId) { }
