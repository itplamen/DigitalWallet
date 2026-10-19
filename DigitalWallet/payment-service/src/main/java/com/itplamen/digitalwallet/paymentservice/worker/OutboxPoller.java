package com.itplamen.digitalwallet.paymentservice.worker;

import com.itplamen.digitalwallet.paymentservice.entity.OutboxMessage;
import com.itplamen.digitalwallet.paymentservice.producer.KafkaEventProducer;
import com.itplamen.digitalwallet.paymentservice.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPoller {

    private final OutboxMessageRepository outboxMessageRepository;
    private final KafkaEventProducer kafkaEventProducer;

    @Scheduled(fixedDelay = 2000)
    public void pollAndPublishEvents() {
        outboxMessageRepository.findUnpublished(50)
                .flatMap(event -> kafkaEventProducer.publish(
                                "payment-events",
                                event.getAggregateId(),
                                event.getPayload().asString(),
                                event.getEventType(),
                                event.getId().toString())
                        .thenReturn(event))
                .collectList()
                .filter(events -> !events.isEmpty())
                .flatMap(events -> {
                    List<UUID> eventIds = events.stream().map(OutboxMessage::getId).toList();
                    return outboxMessageRepository.markAsPublished(eventIds)
                            .doOnSuccess(count ->
                                    log.info("Published and marked {} payment events", count));
                })
                .doOnError(error -> log.error("Failed to publish payment outbox events", error))
                .subscribe();
    }
}
