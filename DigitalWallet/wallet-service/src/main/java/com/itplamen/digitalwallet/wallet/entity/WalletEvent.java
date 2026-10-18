package com.itplamen.digitalwallet.wallet.entity;

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
@Table("wallet_events")
public class WalletEvent {

    @Id
    @Column("event_id")
    private UUID eventId;

    @Column("wallet_id")
    private UUID walletId;

    @Column("version")
    private Long version;

    @Column("event_type")
    private String eventType;

    @Column("payload")
    private Json payload;

    @Column("occurred_at")
    private Instant occurredAt;
}
