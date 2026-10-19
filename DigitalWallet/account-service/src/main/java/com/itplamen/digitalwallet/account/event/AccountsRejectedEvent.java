package com.itplamen.digitalwallet.account.event;

import java.util.UUID;

public record AccountsRejectedEvent(UUID transferId, String reason) { }
