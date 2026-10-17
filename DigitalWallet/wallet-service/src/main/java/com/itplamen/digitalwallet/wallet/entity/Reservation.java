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
@Table("reservations")
public class Reservation {

    @Id
    @Column("id")
    private UUID id;

    @Column("wallet_id")
    private UUID walletId;

    @Column("payment_id")
    private UUID paymentId;

    @Column("amount")
    private BigDecimal amount;

    @Column("status")
    private ReservationStatus status;

    @Column("created_at")
    private Instant createdAt;

    @Column("updated_at")
    private Instant updatedAt;
}
