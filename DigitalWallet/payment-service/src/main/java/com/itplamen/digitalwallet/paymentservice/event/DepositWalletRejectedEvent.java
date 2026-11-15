package com.itplamen.digitalwallet.paymentservice.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DepositWalletRejectedEvent(UUID paymentId, String reason) { }
