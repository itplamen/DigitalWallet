package com.itplamen.digitalwallet.transfer.worker;

import com.itplamen.digitalwallet.transfer.entity.OutboxMessage;
import com.itplamen.digitalwallet.transfer.producer.KafkaEventProducer;
import com.itplamen.digitalwallet.transfer.repository.OutboxMessageRepository;
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

    private final OutboxMessageRepository outboxEventRepository;
    private final KafkaEventProducer kafkaEventProducer;

    @Scheduled(fixedDelay = 2000)
    public void pollAndPublishEvents() {
        outboxEventRepository.findUnpublished(50)
                .flatMap(event ->
                        kafkaEventProducer.publish("transfer-events", event.getAggregateId(), event.getPayload().asString())
                                .thenReturn(event)
                )
                .collectList()
                .filter(events -> !events.isEmpty())
                .flatMap(events -> {
                    List<UUID> eventIds = events.stream().map(OutboxMessage::getId).toList();

                    return outboxEventRepository.markAsPublished(eventIds)
                            .doOnSuccess(updatedCount ->
                                    log.info("Successfully published and marked {} events as published", updatedCount));
                })
                .subscribe();
    }
}