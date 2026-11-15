package com.itplamen.digitalwallet.account.event;

import java.util.UUID;

public record PaymentAccountRejectedEvent(UUID paymentId, String reason) { }
