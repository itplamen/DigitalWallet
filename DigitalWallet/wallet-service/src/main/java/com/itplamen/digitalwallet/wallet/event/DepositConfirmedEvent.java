package com.itplamen.digitalwallet.wallet.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DepositConfirmedEvent(UUID paymentId, UUID accountId, BigDecimal amount, String currency) { }
