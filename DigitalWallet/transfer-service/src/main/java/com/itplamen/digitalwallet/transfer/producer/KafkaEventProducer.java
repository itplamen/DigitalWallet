package com.itplamen.digitalwallet.transfer.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public Mono<SendResult<String, String>> publish(String topic, String key, String payload) {
        return Mono.fromFuture(kafkaTemplate.send(topic, key, payload))
                .doOnSuccess(result -> log.debug("Successfully sent message to topic {} with key {}", topic, key))
                .doOnError(ex -> log.error("Failed to send message to topic {}", topic, ex));
    }
}