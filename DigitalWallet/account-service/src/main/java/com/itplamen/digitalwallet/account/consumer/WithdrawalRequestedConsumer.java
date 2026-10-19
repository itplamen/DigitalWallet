package com.itplamen.digitalwallet.account.consumer;

import com.itplamen.digitalwallet.account.event.WithdrawalRequestedEvent;
import com.itplamen.digitalwallet.account.service.WithdrawalRequestConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class WithdrawalRequestedConsumer {

    private static final String WITHDRAWAL_REQUESTED = "WithdrawalRequested";

    private final ObjectMapper objectMapper;
    private final WithdrawalRequestConsumerService withdrawalRequestConsumerService;

    @KafkaListener(topics = "payment-events", groupId = "account-service-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!WITHDRAWAL_REQUESTED.equals(eventType)) {
                log.debug("Ignoring event type {} from payment-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty WithdrawalRequested payload"));
            }

            return Mono.fromCallable(() -> objectMapper.readValue(record.value(), WithdrawalRequestedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid WithdrawalRequested JSON", error))
                    .flatMap(event -> {
                        if (event.id() == null) {
                            return Mono.error(new IllegalArgumentException("WithdrawalRequested payload has no withdrawal ID"));
                        }

                        return withdrawalRequestConsumerService.handle(eventId, event);
                    });
        }).doOnSuccess(ignored -> log.info("Processed WithdrawalRequested event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process WithdrawalRequested event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
