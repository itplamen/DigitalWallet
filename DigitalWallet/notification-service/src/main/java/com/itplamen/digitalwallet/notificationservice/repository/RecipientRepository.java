package com.itplamen.digitalwallet.notificationservice.repository;

import com.itplamen.digitalwallet.notificationservice.entity.Recipient;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Repository
public interface RecipientRepository extends ReactiveCrudRepository<Recipient, UUID> {

    @Modifying
    @Query("""
            INSERT INTO recipients(account_id, email, name, updated_at)
            VALUES(:accountId, :email, :name, :updatedAt)
            ON CONFLICT (account_id) DO UPDATE
            SET email = EXCLUDED.email, name = EXCLUDED.name, updated_at = EXCLUDED.updated_at
            """)
    Mono<Integer> upsert(
            @Param("accountId") UUID accountId,
            @Param("email") String email,
            @Param("name") String name,
            @Param("updatedAt") Instant updatedAt);
}
