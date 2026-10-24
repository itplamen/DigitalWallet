package com.itplamen.digitalwallet.bankmockservice.dto;

import java.util.UUID;

public record PayoutSucceededWebhook(UUID payoutId, UUID paymentId) { }
