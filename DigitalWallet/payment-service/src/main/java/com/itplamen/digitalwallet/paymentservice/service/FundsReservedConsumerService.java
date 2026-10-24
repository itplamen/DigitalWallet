package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.client.BankMockPayoutClient;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.event.FundsReservedEvent;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FundsReservedConsumerService {

    private static final String CONSUMER = "payment-funds-reserved";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PaymentRepository paymentRepository;
    private final BankMockPayoutClient bankMockPayoutClient;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> handle(UUID eventId, FundsReservedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () -> markFundsReserved(event.paymentId()))
                .flatMap(this::requestPayout);
    }

    private Mono<Void> requestPayout(Payment payment) {
        return bankMockPayoutClient.requestPayout(payment)
                .then(Mono.defer(() -> paymentEventOutboxService.savePayoutSucceeded(payment)));
    }

    private Mono<Payment> markFundsReserved(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.PENDING)
                .flatMap(payment -> {
                    payment.setStatus(PaymentStatus.FUNDS_RESERVED);
                    payment.setUpdatedAt(Instant.now());

                    return paymentRepository.save(payment);
                });
    }
}
