package com.itplamen.digitalwallet.paymentservice.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record PayoutRequest(UUID paymentId, BigDecimal amount, String currency) { }
