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

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("transactions")
public class Transaction {

    @Id
    @Column("id")
    private Long id;

    @Column("wallet_id")
    private Long walletId;

    @Column("transfer_id")
    private Long transferId;

    @Column("amount")
    private BigDecimal amount;

    @Column("type")
    private String type;

    @Column("success")
    private String status;

    @Column("created_at")
    private Instant createdAt;
}
