package com.itplamen.digitalwallet.paymentservice.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record ChargeSucceededWebhookRequest(@NotNull UUID chargeId, @NotNull UUID paymentId) { }
