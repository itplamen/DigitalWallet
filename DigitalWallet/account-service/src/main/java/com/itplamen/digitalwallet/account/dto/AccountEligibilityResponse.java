package com.itplamen.digitalwallet.account.dto;

import java.util.UUID;

public record AccountEligibilityResponse(
        UUID accountId,
        Boolean eligible,
        String reason) {
}
