package com.itplamen.digitalwallet.transfer.repository;

import com.itplamen.digitalwallet.transfer.entity.OutboxMessage;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.UUID;

@Repository
public interface OutboxMessageRepository extends ReactiveCrudRepository<OutboxMessage, UUID> {

    @Query("SELECT * FROM outbox_messages WHERE published_at IS NULL ORDER BY created_at ASC LIMIT :limit")
    Flux<OutboxMessage> findUnpublished(int limit);

    @Modifying
    @Query("UPDATE outbox_messages SET published_at = CURRENT_TIMESTAMP WHERE id IN (:ids) AND published_at IS NULL")
    Mono<Integer> markAsPublished(Collection<UUID> ids);
}