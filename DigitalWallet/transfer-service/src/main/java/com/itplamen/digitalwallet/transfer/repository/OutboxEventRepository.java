package com.itplamen.digitalwallet.transfer.repository;

import com.itplamen.digitalwallet.transfer.entity.OutboxEvent;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

@Repository
public interface OutboxEventRepository extends ReactiveCrudRepository<OutboxEvent, Long> {

    @Query("SELECT * FROM outbox_events WHERE processed = FALSE ORDER BY created_at ASC LIMIT :limit")
    Flux<OutboxEvent> findUnprocessedEvents(int limit);

    @Modifying
    @Query("UPDATE outbox_events SET processed = true, version = version + 1 WHERE id IN (:ids)")
    Mono<Integer> markAsProcessed(Collection<Long> ids);
}