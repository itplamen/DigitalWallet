package com.itplamen.digitalwallet.transfer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("audit_logs")
public class AuditLog {
    @Id
    private Long id;

    @Column("transfer_id")
    private Long transferId;

    @Column("from_status")
    private TransferStatus fromStatus;

    @Column("to_status")
    private TransferStatus toStatus;

    @Column("reason")
    private String reason;

    @Column("created_at")
    private Instant createdAt;
}
