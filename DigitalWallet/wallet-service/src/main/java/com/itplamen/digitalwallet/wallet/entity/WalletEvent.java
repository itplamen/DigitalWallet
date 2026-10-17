package com.itplamen.digitalwallet.wallet.entity;

import io.r2dbc.postgresql.codec.Json;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("wallet_events")
public class WalletEvent {

    @Id
    @Column("id")
    private UUID id;

    @Column("wallet_id")
    private UUID walletId;

    @Column("event_id")
    private UUID eventId;

    @Column("event_type")
    private String eventType;

    @Column("payload")
    private Json payload;

    @Column("created_at")
    private Instant createdAt;

    @Version
    @Column("version")
    private Long version;
}
