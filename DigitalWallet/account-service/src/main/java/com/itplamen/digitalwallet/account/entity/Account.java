package com.itplamen.digitalwallet.account.entity;

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
@Table("accounts")
public class Account {

    @Id
    @Column("id")
    private Long id;

    @Column("account_number")
    private String accountNumber;

    @Column("balance")
    private BigDecimal balance;

    @Column("status")
    private String status;

    @Column("created_at")
    private Instant createdAt;

    @Version
    @Column("version")
    private Long version;
}