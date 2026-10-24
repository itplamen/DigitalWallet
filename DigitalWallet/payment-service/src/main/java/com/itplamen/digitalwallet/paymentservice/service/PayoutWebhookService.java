package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.PayoutSucceededWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PayoutWebhookService {

    private static final String CONSUMER = "payment-payout-succeeded-webhook";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handlePayoutSucceeded(PayoutSucceededWebhookRequest request) {
        return inboxMessageProcessor.process(request.payoutId(), CONSUMER, () -> recordPayout(request));
    }

    private Mono<Void> recordPayout(PayoutSucceededWebhookRequest request) {
        return paymentRepository.findById(request.paymentId())
                .filter(payment -> payment.getStatus() == PaymentStatus.FUNDS_RESERVED)
                .flatMap(payment -> storeBankReference(payment, request))
                .flatMap(paymentEventOutboxService::savePayoutSucceeded);
    }

    private Mono<Payment> storeBankReference(Payment payment, PayoutSucceededWebhookRequest request) {
        payment.setExternalReference(request.payoutId().toString());
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
