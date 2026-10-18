package com.itplamen.digitalwallet.transfer.entity;

import io.r2dbc.postgresql.codec.Json;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("idempotency_keys")
public class IdempotencyKey {

    @Id
    @Column("idempotency_key")
    private String idempotencyKey;

    @Column("request_hash")
    private String requestHash;

    @Column("response_status")
    private Integer responseStatus;

    @Column("response_body")
    private Json responseBody;

    @Column("transfer_id")
    private UUID transferId;

    @Column("created_at")
    private Instant createdAt;

    @Column("expires_at")
    private Instant expiresAt;
}
