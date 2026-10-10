package com.itplamen.digitalwallet.transfer.worker;

import com.itplamen.digitalwallet.transfer.entity.OutboxEvent;
import com.itplamen.digitalwallet.transfer.producer.KafkaEventProducer;
import com.itplamen.digitalwallet.transfer.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaEventProducer kafkaEventProducer;

    @Scheduled(fixedDelay = 2000)
    public void pollAndPublishEvents() {
        outboxEventRepository.findUnprocessedEvents(50)
                .flatMap(event ->
                        kafkaEventProducer.publish("transfer-events", event.getAggregateId(), event.getPayload())
                                .thenReturn(event)
                )
                .collectList()
                .filter(events -> !events.isEmpty())
                .flatMap(events -> {
                    List<Long> eventIds = events.stream().map(OutboxEvent::getId).toList();

                    return outboxEventRepository.markAsProcessed(eventIds)
                            .doOnSuccess(updatedCount ->
                                    log.info("Successfully published and marked {} events as processed", updatedCount));
                })
                .subscribe();
    }
}