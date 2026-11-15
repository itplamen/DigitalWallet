package com.itplamen.digitalwallet.bankmockservice.dto;

import java.util.UUID;

public record ChargeFailedWebhook(UUID chargeId, UUID paymentId, String reason) { }
