package com.itplamen.digitalwallet.account.consumer;

import com.itplamen.digitalwallet.account.event.TransferRequestedEvent;
import com.itplamen.digitalwallet.account.service.TransferRequestConsumerService;
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
public class TransferRequestedConsumer {

    private static final String TRANSFER_REQUESTED = "TransferRequested";

    private final ObjectMapper objectMapper;
    private final TransferRequestConsumerService transferRequestConsumerService;

    @KafkaListener(topics = "transfer-events", groupId = "account-service-group")
    public Mono<Void> consume(ConsumerRecord<String, String> record) {
        return Mono.defer(() -> {
            String eventType = headerValue(record, "eventType");
            if (!TRANSFER_REQUESTED.equals(eventType)) {
                log.debug("Ignoring event type {} from transfer-events", eventType);
                return Mono.empty();
            }

            UUID eventId = UUID.fromString(headerValue(record, "eventId"));
            if (record.value() == null || record.value().isBlank()) {
                return Mono.error(new IllegalArgumentException("Empty TransferRequested payload"));
            }

            return Mono.fromCallable(() ->
                            objectMapper.readValue(record.value(), TransferRequestedEvent.class))
                    .onErrorMap(JacksonException.class,
                            error -> new IllegalArgumentException("Invalid TransferRequested JSON", error))
                    .flatMap(event -> {
                        if (event.id() == null) {
                            return Mono.error(new IllegalArgumentException("TransferRequested payload has no transfer ID"));
                        }
                        return transferRequestConsumerService.handle(eventId, event);
                    });
        }).doOnSuccess(ignored -> log.info("Processed TransferRequested event from topic {} with key {}", record.topic(), record.key()))
                .doOnError(error -> log.error("Failed to process TransferRequested event from topic {} with key {}", record.topic(), record.key(), error));
    }

    private String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing " + name + " header");
        }

        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
