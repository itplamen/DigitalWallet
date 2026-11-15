package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.event.DepositWalletRejectedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DepositWalletRejectedConsumerService {

    private static final String CONSUMER = "payment-deposit-wallet-rejected";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PendingPaymentFailureService pendingPaymentFailureService;

    public Mono<Void> handle(UUID eventId, DepositWalletRejectedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER,
                () -> pendingPaymentFailureService.fail(event.paymentId(), event.reason()));
    }
}
