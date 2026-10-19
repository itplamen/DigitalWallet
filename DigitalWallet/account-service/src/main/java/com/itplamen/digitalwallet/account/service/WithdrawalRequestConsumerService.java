package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.event.WithdrawalRequestedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WithdrawalRequestConsumerService {

    private static final String CONSUMER = "account-withdrawal-validator";

    private final AccountEligibilityValidator accountEligibilityValidator;
    private final InboxMessageProcessor inboxMessageProcessor;
    private final OutboxMessageService outboxMessageService;

    public Mono<Void> handle(UUID eventId, WithdrawalRequestedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () ->
                accountEligibilityValidator.validateWithdrawalAccount(event.accountId(), event.amount())
                        .flatMap(validation -> validation.valid()
                                ? outboxMessageService.saveWithdrawalAccountValidated(event)
                                : Mono.empty()));
    }
}
