package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.event.FundsReservationFailedEvent;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FundsReservationFailedConsumerService {

    private static final String CONSUMER = "payment-funds-reservation-failed";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handle(UUID eventId, FundsReservationFailedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () -> failWithdrawal(event));
    }

    private Mono<Void> failWithdrawal(FundsReservationFailedEvent event) {
        return paymentRepository.findById(event.paymentId())
                .filter(payment -> payment.getStatus() == PaymentStatus.PENDING)
                .flatMap(payment -> markFailed(payment, event.reason()))
                .flatMap(paymentEventOutboxService::saveWithdrawalFailed);
    }

    private Mono<Payment> markFailed(Payment payment, String reason) {
        payment.setStatus(PaymentStatus.FAILED);
        payment.setReason(reason);
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
