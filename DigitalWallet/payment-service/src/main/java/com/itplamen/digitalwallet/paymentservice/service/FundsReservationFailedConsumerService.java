package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.event.FundsReservationFailedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FundsReservationFailedConsumerService {

    private static final String CONSUMER = "payment-funds-reservation-failed";

    private final InboxMessageProcessor inboxMessageProcessor;
    private final PendingPaymentFailureService pendingPaymentFailureService;

    public Mono<Void> handle(UUID eventId, FundsReservationFailedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER,
                () -> pendingPaymentFailureService.fail(event.paymentId(), event.reason()));
    }
}
