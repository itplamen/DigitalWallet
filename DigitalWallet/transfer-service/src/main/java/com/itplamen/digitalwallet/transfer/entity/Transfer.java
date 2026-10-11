package com.itplamen.digitalwallet.transfer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("transfers")
public class Transfer {

    @Id
    @Column("id")
    private Long id;

    @Column("source_wallet_id")
    private Long sourceWalletId;

    @Column("target_wallet_id")
    private Long targetWalletId;

    @Column("amount")
    private BigDecimal amount;

    @Column("currency")
    private String currency;

    @Column("type")
    private TransferType type;

    @Column("status")
    private TransferStatus status;

    @Column("idempotency_key")
    private String idempotencyKey;

    @Column("created_at")
    private Instant createdAt;

    @Column("updated_at")
    private Instant updatedAt;

    @Version
    @Column("version")
    private Long version;
}
