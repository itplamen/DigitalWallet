package com.itplamen.digitalwallet.account.dto;

import com.itplamen.digitalwallet.account.entity.AccountStatus;
import jakarta.validation.constraints.NotNull;

public record UpdateAccountStatusRequest(
        @NotNull AccountStatus status) {
}
