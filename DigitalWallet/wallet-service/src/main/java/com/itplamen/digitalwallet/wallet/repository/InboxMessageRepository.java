package com.itplamen.digitalwallet.wallet.repository;

import com.itplamen.digitalwallet.wallet.entity.InboxMessage;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Repository
public interface InboxMessageRepository extends ReactiveCrudRepository<InboxMessage, UUID> {

    @Modifying
    @Query("""
            INSERT INTO inbox_messages(id, consumer, processed_at)
            VALUES(:id, :consumer, :processedAt)
            ON CONFLICT (id, consumer) DO NOTHING
            """)
    Mono<Integer> insert(
            @Param("id") UUID id,
            @Param("consumer") String consumer,
            @Param("processedAt") Instant processedAt);
}
