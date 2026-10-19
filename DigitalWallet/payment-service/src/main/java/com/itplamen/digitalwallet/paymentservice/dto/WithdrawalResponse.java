package com.itplamen.digitalwallet.paymentservice.dto;

import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;

import java.util.UUID;

public record WithdrawalResponse(UUID withdrawalId, PaymentStatus status) { }
