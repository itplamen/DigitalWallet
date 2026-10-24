package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.event.FundsReleasedEvent;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FundsReleasedConsumerService {

    private static final String CONSUMER = "payment-funds-released";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handle(UUID eventId, FundsReleasedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () -> failWithdrawal(event.paymentId()));
    }

    private Mono<Void> failWithdrawal(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.FUNDS_RESERVED)
                .flatMap(this::markFailed)
                .flatMap(paymentEventOutboxService::saveWithdrawalFailed);
    }

    private Mono<Payment> markFailed(Payment payment) {
        payment.setStatus(PaymentStatus.FAILED);
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
