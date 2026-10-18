package com.itplamen.digitalwallet.wallet.worker;

import com.itplamen.digitalwallet.wallet.producer.WalletEventProducer;
import com.itplamen.digitalwallet.wallet.repository.OutboxMessageRepository;
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
    private final WalletEventProducer walletEventProducer;

    @Scheduled(fixedDelay = 2000)
    public void pollAndPublishEvents() {
        outboxMessageRepository.findUnpublished(50)
                .flatMap(event ->
                        walletEventProducer.publish(
                                        "wallet-events",
                                        event.getAggregateId(),
                                        event.getPayload().asString(),
                                        event.getEventType(),
                                        event.getId().toString())
                                .thenReturn(event)
                )
                .collectList()
                .filter(events -> !events.isEmpty())
                .flatMap(events -> {
                    List<UUID> eventIds = events.stream().map(event -> event.getId()).toList();
                    return outboxMessageRepository.markAsPublished(eventIds)
                            .doOnSuccess(updatedCount -> log.info(
                                    "Successfully published and marked {} wallet outbox events as published",
                                    updatedCount));
                })
                .doOnError(error -> log.error("Failed to relay wallet outbox events", error))
                .subscribe();
    }
}