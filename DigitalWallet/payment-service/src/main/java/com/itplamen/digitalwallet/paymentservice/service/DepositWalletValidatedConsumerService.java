package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.event.DepositWalletValidatedEvent;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DepositWalletValidatedConsumerService {

    private static final String CONSUMER = "payment-deposit-wallet-validated";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;

    public Mono<Void> handle(UUID eventId, DepositWalletValidatedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () -> markWalletValidated(event.paymentId()));
    }

    private Mono<Void> markWalletValidated(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.PENDING)
                .flatMap(payment -> {
                    payment.setStatus(PaymentStatus.WALLET_VALIDATED);
                    payment.setUpdatedAt(Instant.now());

                    return paymentRepository.save(payment);
                })
                .then();
    }
}
