package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.event.AccountsRejectedEvent;
import com.itplamen.digitalwallet.account.event.TransferRequestedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TransferRequestConsumerService {

    private static final String CONSUMER = "account-transfer-validator";

    private final AccountEligibilityValidator accountEligibilityValidator;
    private final InboxMessageProcessor inboxMessageProcessor;
    private final OutboxMessageService outboxMessageService;

    public Mono<Void> handle(UUID eventId, TransferRequestedEvent event) {
        return inboxMessageProcessor.process(eventId, CONSUMER, () ->
                accountEligibilityValidator.validateTransferAccounts(event.sourceAccountId(), event.targetAccountId())
                        .flatMap(validation -> validation.valid()
                                ? outboxMessageService.saveAccountsValidated(event)
                                : outboxMessageService.saveAccountsRejected(
                                        new AccountsRejectedEvent(event.id(), validation.reason()))));
    }
}
