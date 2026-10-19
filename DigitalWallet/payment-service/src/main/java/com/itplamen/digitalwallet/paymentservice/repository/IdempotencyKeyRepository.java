package com.itplamen.digitalwallet.paymentservice.repository;

import com.itplamen.digitalwallet.paymentservice.entity.IdempotencyKey;
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
public interface IdempotencyKeyRepository extends ReactiveCrudRepository<IdempotencyKey, String> {

    @Modifying
    @Query("""
            INSERT INTO idempotency_keys(idempotency_key, request_hash, response_status, response_body, payment_id, created_at, expires_at)
            VALUES(:idempotencyKey, :requestHash, :responseStatus, :responseBody, :paymentId, :createdAt, :expiresAt)
            """)
    Mono<Integer> create(
            @Param("idempotencyKey") String idempotencyKey,
            @Param("requestHash") String requestHash,
            @Param("responseStatus") Integer responseStatus,
            @Param("responseBody") Json responseBody,
            @Param("paymentId") UUID paymentId,
            @Param("createdAt") Instant createdAt,
            @Param("expiresAt") Instant expiresAt);
}
