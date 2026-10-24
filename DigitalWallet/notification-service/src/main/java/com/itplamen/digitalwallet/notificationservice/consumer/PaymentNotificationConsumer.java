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
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentNotificationConsumer {

    private static final String WITHDRAWAL_COMPLETED = "WithdrawalCompleted";

    private final ObjectMapper objectMapper;
    private final NotificationDeliveryService notificationDeliveryService;

    @KafkaListener(topics = "payment-events", groupId = "notification-service-group")
    public Mono<Void> consumePaymentEvent(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!WITHDRAWAL_COMPLETED.equals(eventType)) {
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty WithdrawalCompleted payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WithdrawalCompletedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid WithdrawalCompleted JSON", error))
                    .flatMap(event -> notificationDeliveryService.processWithdrawalCompleted(
                            eventId, event.paymentId(), event.accountId(), event.amount(), event.currency()));
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WithdrawalCompletedEvent(UUID paymentId, UUID accountId, BigDecimal amount, String currency) { }
}
