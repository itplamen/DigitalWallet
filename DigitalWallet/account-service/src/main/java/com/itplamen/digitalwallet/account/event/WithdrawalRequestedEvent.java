package com.itplamen.digitalwallet.account.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WithdrawalRequestedEvent(UUID id, UUID accountId, BigDecimal amount) { }
