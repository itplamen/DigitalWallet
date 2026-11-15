package com.itplamen.digitalwallet.bankmockservice.dto;

import java.util.UUID;

public record ChargeSucceededWebhook(UUID chargeId, UUID paymentId) { }
