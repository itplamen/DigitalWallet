package com.itplamen.digitalwallet.account.entity;

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
@Table("inbox_messages")
public class InboxMessage {

    @Id
    @Column("id")
    private UUID id;

    @Column("consumer")
    private String consumer;

    @Column("processed_at")
    private Instant processedAt;
}
