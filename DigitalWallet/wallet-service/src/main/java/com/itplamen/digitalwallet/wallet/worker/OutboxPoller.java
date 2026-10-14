package com.itplamen.digitalwallet.wallet.worker;

import com.itplamen.digitalwallet.wallet.producer.WalletEventProducer;
import com.itplamen.digitalwallet.wallet.repository.OutboxMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPoller {

    private final OutboxMessageRepository outboxMessageRepository;
    private final WalletEventProducer walletEventProducer;

    @Scheduled(fixedDelay = 2000)
    public void pollAndPublishEvents() {
        outboxMessageRepository.findUnprocessed(50)
                .flatMap(event ->
                        walletEventProducer.publish(
                                        "wallet-events",
                                        event.getAggregateId(),
                                        event.getPayload(),
                                        event.getEventType())
                                .thenReturn(event)
                )
                .collectList()
                .filter(events -> !events.isEmpty())
                .flatMap(events -> {
                    events.forEach(e -> e.setProcessed(true));
                    return outboxMessageRepository.saveAll(events)
                            .collectList()
                            .doOnSuccess(saved -> log.info("Successfully published and marked {} wallet outbox events as processed", saved.size()));
                })
                .subscribe();
    }
}