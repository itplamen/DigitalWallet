package com.itplamen.digitalwallet.bankmockservice.dto;

import java.util.UUID;

public record PayoutFailedWebhook(UUID payoutId, UUID paymentId, String reason) { }
