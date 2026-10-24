package com.itplamen.digitalwallet.notificationservice.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record NotificationEvent(
        UUID eventId,
        UUID transferId,
        UUID paymentId,
        String status,
        String reason,
        BigDecimal amount,
        String currency) { }
