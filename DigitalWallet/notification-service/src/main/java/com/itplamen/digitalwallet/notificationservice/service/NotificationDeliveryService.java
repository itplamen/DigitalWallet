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

                    NotificationEvent notificationEvent = new NotificationEvent(eventId, transferId, status, reason, amount, currency);

                    return Flux.fromIterable(List.of(sourceAccountId, targetAccountId).stream().distinct().toList())
                            .concatMap(accountId -> deliverToAccount(accountId, notificationEvent, reason))
                            .then();
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> deliverToAccount(UUID accountId, NotificationEvent event, String reason) {
        Instant now = Instant.now();
        return recipientRepository.findById(accountId)
                .defaultIfEmpty(Recipient.builder().accountId(accountId).build())
                .flatMap(recipient -> recipientRepository.upsert(accountId, recipient.getEmail(), recipient.getName(), now)
                        .then(deliverEmail(accountId, recipient, event, reason, now))
                        .then(deliverSse(accountId, event, now)));
    }

    private Mono<Void> deliverEmail(UUID accountId, Recipient recipient, NotificationEvent event, String reason, Instant createdAt) {
        String template = "TRANSFER_" + event.status();
        String subject = "Digital Wallet transfer " + event.status().toLowerCase(Locale.ROOT);
        String body = emailBody(recipient, event, reason);

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

    private Mono<Void> deliverSse(UUID accountId, NotificationEvent event, Instant createdAt) {
        boolean emitted = sseNotificationHub.emit(accountId, event);
        NotificationRecord record = notification(
                accountId,
                "SSE",
                "TRANSFER_" + event.status(),
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

    private String emailBody(Recipient recipient, NotificationEvent event, String reason) {
        String greeting = recipient.getName() == null || recipient.getName().isBlank()
                ? "Hello,"
                : "Hello " + recipient.getName() + ",";
        String transferDetails = "Transfer " + event.transferId() + " is "
                + event.status().toLowerCase(Locale.ROOT) + ".";
        String amount = event.amount() == null ? "" : "\nAmount: " + event.amount() + " " + event.currency();
        String failureReason = reason == null || reason.isBlank() ? "" : "\nReason: " + reason;

        return greeting + "\n\n" + transferDetails + amount + failureReason;
    }

    private String errorMessage(Throwable error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return error.getClass().getSimpleName();
        }

        return message.length() <= 255 ? message : message.substring(0, 255);
    }

    public record AccountCreatedEvent(UUID accountId, String email, String name) { }
}
