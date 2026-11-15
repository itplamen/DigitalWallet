package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.ChargeSucceededWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ChargeWebhookService {

    private static final String SUCCEEDED_CONSUMER = "payment-charge-succeeded-webhook";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handleChargeSucceeded(ChargeSucceededWebhookRequest request) {
        return inboxMessageProcessor.process(request.chargeId(), SUCCEEDED_CONSUMER,
                () -> markCharged(request.paymentId(), request.chargeId()));
    }

    private Mono<Void> markCharged(UUID paymentId, UUID chargeId) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.WALLET_VALIDATED)
                .flatMap(payment -> storeCharge(payment, chargeId))
                .flatMap(paymentEventOutboxService::saveDepositConfirmed);
    }

    private Mono<Payment> storeCharge(Payment payment, UUID chargeId) {
        payment.setStatus(PaymentStatus.CHARGED);
        payment.setExternalReference(chargeId.toString());
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
