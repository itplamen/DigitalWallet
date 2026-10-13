package com.itplamen.digitalwallet.account.repository;

import com.itplamen.digitalwallet.account.entity.OutboxMessage;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

@Repository
public interface OutboxMessageRepository extends ReactiveCrudRepository<OutboxMessage, Long> {

    @Query("SELECT * FROM outbox_messages WHERE processed = FALSE ORDER BY created_at ASC LIMIT :limit")
    Flux<OutboxMessage> findUnprocessed(int limit);

    @Modifying
    @Query("UPDATE outbox_messages SET processed = true, version = version + 1 WHERE id IN (:ids)")
    Mono<Integer> markAsProcessed(Collection<Long> ids);
}