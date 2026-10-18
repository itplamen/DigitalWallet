package com.itplamen.digitalwallet.transfer.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public Mono<SendResult<String, String>> publish(String topic, String key, String payload, String eventType, String eventId) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        record.headers().add(new RecordHeader("eventType", eventType.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventId", eventId.getBytes(StandardCharsets.UTF_8)));

        return Mono.fromFuture(kafkaTemplate.send(record))
                .doOnSuccess(result -> log.debug("Successfully sent message to topic {} with key {}", topic, key))
                .doOnError(ex -> log.error("Failed to send message to topic {}", topic, ex));
    }
}