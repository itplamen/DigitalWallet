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
public class TransferNotificationConsumer {

    private final ObjectMapper objectMapper;
    private final NotificationDeliveryService notificationDeliveryService;

    @KafkaListener(topics = "transfer-events", groupId = "notification-service-group")
    public Mono<Void> consumeTransferEvent(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!"TransferCompleted".equals(eventType) && !"TransferFailed".equals(eventType)) {
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty transfer event payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), TransferEvent.class))
                    .onErrorMap(JacksonException.class, error -> new IllegalArgumentException("Invalid transfer event JSON", error))
                    .flatMap(event -> {
                        if (eventType.equals("TransferFailed") && (event.reason() == null || event.reason().isBlank())) {
                            return Mono.error(new IllegalArgumentException("TransferFailed payload must include a reason"));
                        }

                        return notificationDeliveryService.processTransfer(
                                eventId,
                                event.transferId(),
                                event.sourceAccountId(),
                                event.targetAccountId(),
                                eventType.equals("TransferCompleted") ? "COMPLETED" : "FAILED",
                                event.reason(),
                                event.amount(),
                                event.currency());
                    });
        }).doOnError(error -> log.error(
                "Failed to process transfer notification event from topic [{}], partition [{}]",
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
    public record TransferEvent(
            UUID transferId,
            UUID sourceAccountId,
            UUID targetAccountId,
            BigDecimal amount,
            String currency,
            String reason) { }
}
