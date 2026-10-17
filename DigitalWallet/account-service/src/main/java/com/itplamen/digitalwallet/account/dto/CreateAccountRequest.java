package com.itplamen.digitalwallet.account.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateAccountRequest(
        @NotBlank @Size(max = 50) String accountNumber,
        @Size(max = 255) String customerName,
        @Email @Size(max = 255) String email) {
}
