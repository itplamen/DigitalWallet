package com.itplamen.digitalwallet.notificationservice.service;

import com.itplamen.digitalwallet.notificationservice.dto.NotificationEvent;
import com.itplamen.digitalwallet.notificationservice.entity.NotificationRecord;
import com.itplamen.digitalwallet.notificationservice.entity.Recipient;
import com.itplamen.digitalwallet.notificationservice.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.notificationservice.repository.NotificationRepository;
import com.itplamen.digitalwallet.notificationservice.repository.RecipientRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationDeliveryService {

    private static final String TRANSFER_CONSUMER = "notification-transfer-events";
    private static final String PAYMENT_CONSUMER = "notification-payment-events";
    private static final String ACCOUNT_CONSUMER = "notification-account-events";

    private final InboxMessageRepository inboxMessageRepository;
    private final NotificationRepository notificationRepository;
    private final RecipientRepository recipientRepository;
    private final EmailSender emailSender;
    private final SseNotificationHub sseNotificationHub;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> processAccountCreated(UUID eventId, AccountCreatedEvent event) {
        if (event.accountId() == null) {
            return Mono.error(new IllegalArgumentException("AccountCreated payload has no account ID"));
        }

        return inboxMessageRepository.insert(eventId, ACCOUNT_CONSUMER, Instant.now())
                .flatMap(inserted -> inserted == 0
                        ? Mono.empty()
                        : recipientRepository.upsert(event.accountId(), event.email(), event.name(), Instant.now()).then())
                .as(transactionalOperator::transactional);
    }

    public Mono<Void> processTransfer(
            UUID eventId,
            UUID transferId,
            UUID sourceAccountId,
            UUID targetAccountId,
            String status,
            String reason,
            BigDecimal amount,
            String currency) {
        if (transferId == null || sourceAccountId == null || targetAccountId == null) {
            return Mono.error(new IllegalArgumentException("Transfer notification payload must contain transfer and account IDs"));
        }

        return inboxMessageRepository.insert(eventId, TRANSFER_CONSUMER, Instant.now())
                .flatMap(inserted -> {
                    if (inserted == 0) {
                        return Mono.empty();
                    }

                    NotificationEvent notificationEvent = new NotificationEvent(eventId, transferId, null, status, reason, amount, currency);
                    NotificationContent content = transferContent(notificationEvent);

                    return Flux.fromIterable(List.of(sourceAccountId, targetAccountId).stream().distinct().toList())
                            .concatMap(accountId -> deliverToAccount(accountId, notificationEvent, content))
                            .then();
                })
                .as(transactionalOperator::transactional);
    }

    public Mono<Void> processWithdrawal(
            UUID eventId,
            UUID paymentId,
            UUID accountId,
            String status,
            String reason,
            BigDecimal amount,
            String currency) {
        if (paymentId == null || accountId == null) {
            return Mono.error(new IllegalArgumentException("Withdrawal notification payload must contain payment and account IDs"));
        }

        return inboxMessageRepository.insert(eventId, PAYMENT_CONSUMER, Instant.now())
                .flatMap(inserted -> {
                    if (inserted == 0) {
                        return Mono.empty();
                    }

                    NotificationEvent notificationEvent = new NotificationEvent(eventId, null, paymentId, status, reason, amount, currency);

                    return deliverToAccount(accountId, notificationEvent, withdrawalContent(notificationEvent));
                })
                .as(transactionalOperator::transactional);
    }

    private NotificationContent transferContent(NotificationEvent event) {
        String status = event.status().toLowerCase(Locale.ROOT);

        return new NotificationContent(
                "TRANSFER_" + event.status(),
                "Digital Wallet transfer " + status,
                "Transfer " + event.transferId() + " is " + status + "." + amountLine(event) + reasonLine(event));
    }

    private NotificationContent withdrawalContent(NotificationEvent event) {
        String status = event.status().toLowerCase(Locale.ROOT);

        return new NotificationContent(
                "WITHDRAWAL_" + event.status(),
                "Digital Wallet withdrawal " + status,
                "Withdrawal " + event.paymentId() + " is " + status + "." + amountLine(event) + reasonLine(event));
    }

    private String reasonLine(NotificationEvent event) {
        return event.reason() == null || event.reason().isBlank() ? "" : "\nReason: " + event.reason();
    }

    private String amountLine(NotificationEvent event) {
        return event.amount() == null ? "" : "\nAmount: " + event.amount() + " " + event.currency();
    }

    private Mono<Void> deliverToAccount(UUID accountId, NotificationEvent event, NotificationContent content) {
        Instant now = Instant.now();
        return recipientRepository.findById(accountId)
                .defaultIfEmpty(Recipient.builder().accountId(accountId).build())
                .flatMap(recipient -> recipientRepository.upsert(accountId, recipient.getEmail(), recipient.getName(), now)
                        .then(deliverEmail(accountId, recipient, event, content, now))
                        .then(deliverSse(accountId, event, content, now)));
    }

    private Mono<Void> deliverEmail(UUID accountId, Recipient recipient, NotificationEvent event, NotificationContent content, Instant createdAt) {
        String template = content.template();
        String subject = content.subject();
        String body = emailBody(recipient, content.details());

        Mono<NotificationRecord> result;
        if (recipient.getEmail() == null || recipient.getEmail().isBlank()) {
            result = Mono.just(notification(
                    accountId, "EMAIL", template, event.eventId(), "FAILED",
                    "No email address registered for recipient", createdAt, null));
        } else {
            result = emailSender.send(recipient.getEmail(), subject, body)
                    .thenReturn(notification(
                            accountId, "EMAIL", template, event.eventId(), "SENT",
                            null, createdAt, Instant.now()))
                    .onErrorResume(error -> {
                        log.warn("Email delivery failed for notification event {}", event.eventId(), error);
                        return Mono.just(notification(
                                accountId, "EMAIL", template, event.eventId(), "FAILED",
                                errorMessage(error), createdAt, null));
                    });
        }

        return result.flatMap(notificationRepository::save).then();
    }

    private Mono<Void> deliverSse(UUID accountId, NotificationEvent event, NotificationContent content, Instant createdAt) {
        boolean emitted = sseNotificationHub.emit(accountId, event);
        NotificationRecord record = notification(
                accountId,
                "SSE",
                content.template(),
                event.eventId(),
                emitted ? "SENT" : "FAILED",
                emitted ? null : "No active SSE connection for recipient",
                createdAt,
                emitted ? Instant.now() : null);

        return notificationRepository.save(record).then();
    }

    private NotificationRecord notification(
            UUID accountId,
            String channel,
            String template,
            UUID eventId,
            String status,
            String error,
            Instant createdAt,
            Instant sentAt) {
        return NotificationRecord.builder()
                .id(UUID.randomUUID())
                .accountId(accountId)
                .channel(channel)
                .template(template)
                .eventId(eventId)
                .status(status)
                .error(error)
                .createdAt(createdAt)
                .sentAt(sentAt)
                .build();
    }

    private String emailBody(Recipient recipient, String details) {
        String greeting = recipient.getName() == null || recipient.getName().isBlank()
                ? "Hello,"
                : "Hello " + recipient.getName() + ",";

        return greeting + "\n\n" + details;
    }

    private String errorMessage(Throwable error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return error.getClass().getSimpleName();
        }

        return message.length() <= 255 ? message : message.substring(0, 255);
    }

    private record NotificationContent(String template, String subject, String details) { }

    public record AccountCreatedEvent(UUID accountId, String email, String name) { }
}
