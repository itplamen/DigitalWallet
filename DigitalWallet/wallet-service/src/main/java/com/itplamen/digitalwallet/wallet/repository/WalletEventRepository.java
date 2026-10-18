package com.itplamen.digitalwallet.wallet.repository;

import com.itplamen.digitalwallet.wallet.entity.WalletEvent;
import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Repository
public interface WalletEventRepository extends ReactiveCrudRepository<WalletEvent, UUID> {

    @Modifying
    @Query("""
            INSERT INTO wallet_events(wallet_id, version, event_id, event_type, payload, occurred_at)
            VALUES(:walletId, :version, :eventId, :eventType, :payload, :occurredAt)
            """)
    Mono<Long> append(
            @Param("walletId") UUID walletId,
            @Param("version") Long version,
            @Param("eventId") UUID eventId,
            @Param("eventType") String eventType,
            @Param("payload") Json payload,
            @Param("occurredAt") Instant occurredAt);
}
