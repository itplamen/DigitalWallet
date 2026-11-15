package com.itplamen.digitalwallet.paymentservice.event;

import java.math.BigDecimal;
import java.util.UUID;

public record DepositConfirmedEvent(UUID paymentId, UUID accountId, BigDecimal amount, String currency) { }
