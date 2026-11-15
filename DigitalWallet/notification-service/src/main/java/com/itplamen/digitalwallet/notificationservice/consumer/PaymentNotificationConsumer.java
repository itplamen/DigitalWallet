package com.itplamen.digitalwallet.notificationservice.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.notificationservice.service.NotificationDeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentNotificationConsumer {

    private static final Map<String, PaymentOutcome> OUTCOMES = Map.of(
            "WithdrawalCompleted", new PaymentOutcome("WITHDRAWAL", "COMPLETED"),
            "WithdrawalFailed", new PaymentOutcome("WITHDRAWAL", "FAILED"),
            "DepositCompleted", new PaymentOutcome("DEPOSIT", "COMPLETED"),
            "DepositFailed", new PaymentOutcome("DEPOSIT", "FAILED"));

    private final ObjectMapper objectMapper;
    private final NotificationDeliveryService notificationDeliveryService;

    @KafkaListener(topics = "payment-events", groupId = "notification-service-group")
    public Mono<Void> consumePaymentEvent(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            PaymentOutcome outcome = OUTCOMES.get(eventType);
            if (outcome == null) {
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty " + eventType + " payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), PaymentEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid " + eventType + " JSON", error))
                    .flatMap(event -> notificationDeliveryService.processPayment(
                            eventId,
                            outcome.kind(),
                            event.paymentId(),
                            event.accountId(),
                            outcome.status(),
                            event.reason(),
                            event.amount(),
                            event.currency()));
        }).doOnError(error -> log.error(
                "Failed to process payment notification event from topic [{}], partition [{}]",
                record.topic(), record.partition(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private record PaymentOutcome(String kind, String status) { }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PaymentEvent(UUID paymentId, UUID accountId, BigDecimal amount, String currency, String reason) { }
}
