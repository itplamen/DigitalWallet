package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.ChargeFailedWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.dto.ChargeSucceededWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;

@Service
@RequiredArgsConstructor
public class ChargeWebhookService {

    private static final String SUCCEEDED_CONSUMER = "payment-charge-succeeded-webhook";
    private static final String FAILED_CONSUMER = "payment-charge-failed-webhook";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handleChargeSucceeded(ChargeSucceededWebhookRequest request) {
        return inboxMessageProcessor.process(request.chargeId(), SUCCEEDED_CONSUMER,
                () -> recordCharge(request.paymentId(), request.chargeId(), PaymentStatus.CHARGED, null,
                        paymentEventOutboxService::saveDepositConfirmed));
    }

    public Mono<Void> handleChargeFailed(ChargeFailedWebhookRequest request) {
        return inboxMessageProcessor.process(request.chargeId(), FAILED_CONSUMER,
                () -> recordCharge(request.paymentId(), request.chargeId(), PaymentStatus.FAILED, request.reason(),
                        paymentEventOutboxService::saveDepositFailed));
    }

    private Mono<Void> recordCharge(
            UUID paymentId,
            UUID chargeId,
            PaymentStatus status,
            String reason,
            Function<Payment, Mono<Void>> saveOutboxEvent) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.WALLET_VALIDATED)
                .flatMap(payment -> storeCharge(payment, chargeId, status, reason))
                .flatMap(saveOutboxEvent);
    }

    private Mono<Payment> storeCharge(Payment payment, UUID chargeId, PaymentStatus status, String reason) {
        payment.setStatus(status);
        payment.setExternalReference(chargeId.toString());
        payment.setReason(reason);
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
