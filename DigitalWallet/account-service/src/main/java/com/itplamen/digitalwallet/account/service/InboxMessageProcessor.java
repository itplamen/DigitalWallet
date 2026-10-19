package com.itplamen.digitalwallet.account.service;

import com.itplamen.digitalwallet.account.repository.InboxMessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

@Service
@RequiredArgsConstructor
public class InboxMessageProcessor {

    private final InboxMessageRepository inboxMessageRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> process(UUID eventId, String consumer, Supplier<Mono<Void>> operation) {
        return inboxMessageRepository.insert(eventId, consumer, Instant.now())
                .flatMap(inserted -> inserted == 0
                        ? Mono.empty()
                        : Mono.defer(operation))
                .as(transactionalOperator::transactional);
    }
}
