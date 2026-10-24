package com.itplamen.digitalwallet.wallet.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PayoutSucceededEvent(UUID paymentId) { }
