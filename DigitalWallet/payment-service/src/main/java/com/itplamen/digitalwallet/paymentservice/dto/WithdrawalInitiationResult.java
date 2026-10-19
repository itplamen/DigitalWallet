package com.itplamen.digitalwallet.paymentservice.dto;

import com.itplamen.digitalwallet.paymentservice.entity.Payment;

public record WithdrawalInitiationResult(Payment payment, int responseStatus) { }
