package com.itplamen.digitalwallet.wallet.event;

import java.math.BigDecimal;
import java.util.UUID;

public record FundsReservedEvent(UUID paymentId, UUID walletId, UUID accountId, BigDecimal amount, String currency) { }
