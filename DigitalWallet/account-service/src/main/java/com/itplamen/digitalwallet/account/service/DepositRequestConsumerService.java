package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.event.DepositRequestedEvent;
import com.itplamen.digitalwallet.account.event.PaymentAccountRejectedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DepositRequestConsumerService {

    private static final String CONSUMER = "account-deposit-validator";

    private final AccountEligibilityValidator accountEligibilityValidator;
    private final InboxMessageProcessor inboxMessageProcessor;
    private final OutboxMessageService outboxMessageService;

    public Mono<Void> handle(UUID eventId, DepositRequestedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () ->
                accountEligibilityValidator.validatePaymentAccount(event.accountId(), event.amount())
                        .flatMap(validation -> validation.valid()
                                ? outboxMessageService.saveDepositAccountValidated(event)
                                : outboxMessageService.savePaymentAccountRejected(
                                        new PaymentAccountRejectedEvent(event.id(), validation.reason()))));
    }
}
