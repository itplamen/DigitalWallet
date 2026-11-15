package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.event.WalletCreditedEvent;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WalletCreditedConsumerService {

    private static final String CONSUMER = "payment-wallet-credited";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handle(UUID eventId, WalletCreditedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () -> completeDeposit(event.paymentId()));
    }

    private Mono<Void> completeDeposit(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.CHARGED)
                .flatMap(this::markCompleted)
                .flatMap(paymentEventOutboxService::saveDepositCompleted);
    }

    private Mono<Payment> markCompleted(Payment payment) {
        payment.setStatus(PaymentStatus.COMPLETED);
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
