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
@Table("recipients")
public class Recipient {

    @Id
    @Column("account_id")
    private UUID accountId;

    @Column("email")
    private String email;

    @Column("name")
    private String name;

    @Column("updated_at")
    private Instant updatedAt;
}
