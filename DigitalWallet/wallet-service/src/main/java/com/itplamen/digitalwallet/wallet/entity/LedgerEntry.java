package com.itplamen.digitalwallet.wallet.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("ledger_entries")
public class LedgerEntry {

    @Id
    @Column("id")
    private Long id;

    @Column("wallet_id")
    private UUID walletId;

    @Column("transfer_id")
    private UUID transferId;

    @Column("entry_type")
    private String entryType;

    @Column("amount")
    private BigDecimal amount;

    @Column("currency")
    private String currency;

    @Column("created_at")
    private Instant createdAt;
}
