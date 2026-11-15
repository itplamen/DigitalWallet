package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.PaymentRequest;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentType;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public record PaymentInitiation<R>(
        PaymentType type,
        PaymentRequest request,
        int responseStatus,
        Class<R> responseType,
        Function<Payment, R> responseFactory,
        Function<Payment, Mono<Void>> outboxWriter) { }
