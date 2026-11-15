package com.itplamen.digitalwallet.wallet.event;

import java.util.UUID;

public record FundsReservationFailedEvent(UUID paymentId, String reason) { }
