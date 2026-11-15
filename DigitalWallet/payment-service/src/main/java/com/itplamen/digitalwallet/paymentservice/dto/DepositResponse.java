package com.itplamen.digitalwallet.paymentservice.dto;

import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;

import java.util.UUID;

public record DepositResponse(UUID depositId, PaymentStatus status) { }
