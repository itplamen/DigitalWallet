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
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("accounts")
public class Account {

    @Id
    @Column("id")
    private UUID id;

    @Column("account_number")
    private String accountNumber;

    @Column("customer_name")
    private String customerName;

    @Column("email")
    private String email;

    @Column("status")
    private AccountStatus status;

    @Column("kyc_checked")
    private Boolean kycChecked;

    @Column("transfer_limit")
    private BigDecimal transferLimit;

    @Column("created_at")
    private Instant createdAt;

    @Column("updated_at")
    private Instant updatedAt;

    @Version
    @Column("version")
    private Long version;
}