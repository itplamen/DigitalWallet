package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.event.FundsCapturedEvent;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FundsCapturedConsumerService {

    private static final String CONSUMER = "payment-funds-captured";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handle(UUID eventId, FundsCapturedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () -> completeWithdrawal(event.paymentId()));
    }

    private Mono<Void> completeWithdrawal(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.FUNDS_RESERVED)
                .flatMap(this::markCompleted)
                .flatMap(paymentEventOutboxService::saveWithdrawalCompleted);
    }

    private Mono<Payment> markCompleted(Payment payment) {
        payment.setStatus(PaymentStatus.COMPLETED);
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
