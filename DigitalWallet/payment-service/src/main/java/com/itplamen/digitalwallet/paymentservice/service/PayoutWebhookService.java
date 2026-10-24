package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.PayoutFailedWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.dto.PayoutSucceededWebhookRequest;
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
public class PayoutWebhookService {

    private static final String SUCCEEDED_CONSUMER = "payment-payout-succeeded-webhook";
    private static final String FAILED_CONSUMER = "payment-payout-failed-webhook";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handlePayoutSucceeded(PayoutSucceededWebhookRequest request) {
        return inboxMessageProcessor.process(request.payoutId(), SUCCEEDED_CONSUMER,
                () -> recordPayout(request.paymentId(), request.payoutId(), paymentEventOutboxService::savePayoutSucceeded));
    }

    public Mono<Void> handlePayoutFailed(PayoutFailedWebhookRequest request) {
        return inboxMessageProcessor.process(request.payoutId(), FAILED_CONSUMER,
                () -> recordPayout(request.paymentId(), request.payoutId(),
                        payment -> paymentEventOutboxService.savePayoutFailed(payment, request.reason())));
    }

    private Mono<Void> recordPayout(UUID paymentId, UUID payoutId, Function<Payment, Mono<Void>> saveOutboxEvent) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.FUNDS_RESERVED)
                .flatMap(payment -> storeBankReference(payment, payoutId))
                .flatMap(saveOutboxEvent);
    }

    private Mono<Payment> storeBankReference(Payment payment, UUID payoutId) {
        payment.setExternalReference(payoutId.toString());
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
