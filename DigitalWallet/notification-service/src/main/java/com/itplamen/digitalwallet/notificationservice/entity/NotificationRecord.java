package com.itplamen.digitalwallet.notificationservice.entity;

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
@Table("notifications")
public class NotificationRecord {

    @Id
    @Column("id")
    private UUID id;

    @Column("account_id")
    private UUID accountId;

    @Column("channel")
    private String channel;

    @Column("template")
    private String template;

    @Column("event_id")
    private UUID eventId;

    @Column("status")
    private String status;

    @Column("error")
    private String error;

    @Column("created_at")
    private Instant createdAt;

    @Column("sent_at")
    private Instant sentAt;
}
