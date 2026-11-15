package com.itplamen.digitalwallet.wallet.event;

import java.util.UUID;

public record DepositWalletRejectedEvent(UUID paymentId, String reason) { }
