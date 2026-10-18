package com.itplamen.digitalwallet.account.worker;

import com.itplamen.digitalwallet.account.entity.OutboxMessage;
import com.itplamen.digitalwallet.account.producer.KafkaEventProducer;
import com.itplamen.digitalwallet.account.repository.OutboxMessageRepository;
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

    private final OutboxMessageRepository outboxRepository;
    private final KafkaEventProducer kafkaEventProducer;

    @Scheduled(fixedDelay = 2000)
    public void pollAndPublishEvents() {
        outboxRepository.findUnpublished(50)
                .flatMap(event -> kafkaEventProducer.publish(
                                "account-events",
                                event.getAggregateId(),
                                event.getPayload().asString(),
                                event.getEventType(),
                                event.getId().toString())
                        .thenReturn(event))
                .collectList()
                .filter(events -> !events.isEmpty())
                .flatMap(events -> {
                    List<UUID> eventIds = events.stream().map(OutboxMessage::getId).toList();
                    return outboxRepository.markAsPublished(eventIds)
                            .doOnSuccess(count -> log.info("Successfully published and marked {} account events", count));
                })
                .doOnError(error -> log.error("Failed to publish account outbox events", error))
                .subscribe();
    }
}
