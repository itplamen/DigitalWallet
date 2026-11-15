package com.itplamen.digitalwallet.paymentservice.dto;

public record PaymentInitiationResult<T>(T response, int responseStatus) { }
