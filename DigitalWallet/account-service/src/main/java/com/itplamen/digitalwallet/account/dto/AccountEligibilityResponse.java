package com.itplamen.digitalwallet.account.dto;

public record AccountEligibilityResponse(
        Long accountId,
        Boolean eligible,
        String reason) {
}
