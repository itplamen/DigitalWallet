package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.DepositResponse;
import com.itplamen.digitalwallet.paymentservice.dto.PaymentInitiationResult;
import com.itplamen.digitalwallet.paymentservice.dto.PaymentRequest;
import com.itplamen.digitalwallet.paymentservice.dto.WithdrawalResponse;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private static final int CREATED_RESPONSE_STATUS = 201;

    private final IdempotentPaymentInitiator idempotentPaymentInitiator;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<PaymentInitiationResult<WithdrawalResponse>> createWithdrawal(String idempotencyKey, PaymentRequest request) {
        return idempotentPaymentInitiator.initiate(idempotencyKey, new PaymentInitiation<>(
                PaymentType.WITHDRAWAL,
                request,
                CREATED_RESPONSE_STATUS,
                WithdrawalResponse.class,
                payment -> new WithdrawalResponse(payment.getId(), payment.getStatus()),
                paymentEventOutboxService::saveWithdrawalRequested));
    }

    public Mono<PaymentInitiationResult<DepositResponse>> createDeposit(String idempotencyKey, PaymentRequest request) {
        return idempotentPaymentInitiator.initiate(idempotencyKey, new PaymentInitiation<>(
                PaymentType.DEPOSIT,
                request,
                CREATED_RESPONSE_STATUS,
                DepositResponse.class,
                payment -> new DepositResponse(payment.getId(), payment.getStatus()),
                paymentEventOutboxService::saveDepositRequested));
    }
}
