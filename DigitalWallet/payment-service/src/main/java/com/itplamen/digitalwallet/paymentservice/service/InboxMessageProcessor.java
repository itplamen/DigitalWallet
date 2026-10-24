package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.repository.InboxMessageRepository;
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

    public <T> Mono<T> process(UUID eventId, String consumer, Supplier<Mono<T>> operation) {
        return inboxMessageRepository.insert(eventId, consumer, Instant.now())
                .flatMap(inserted -> inserted == 0
                        ? Mono.<T>empty()
                        : Mono.defer(operation))
                .as(transactionalOperator::transactional);
    }
}
