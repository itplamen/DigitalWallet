package com.itplamen.digitalwallet.paymentservice.repository;

import com.itplamen.digitalwallet.paymentservice.entity.OutboxMessage;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface OutboxMessageRepository extends ReactiveCrudRepository<OutboxMessage, UUID> {

    Mono<OutboxMessage> findByAggregateIdAndEventType(String aggregateId, String eventType);
}
