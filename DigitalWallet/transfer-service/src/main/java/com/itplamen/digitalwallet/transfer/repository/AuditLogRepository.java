package com.itplamen.digitalwallet.transfer.repository;

import com.itplamen.digitalwallet.transfer.entity.AuditLog;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface AuditLogRepository extends ReactiveCrudRepository<AuditLog, Long> {

    @Query("SELECT * FROM audit_logs WHERE transfer_id = :transferId")
    Flux<AuditLog> findByTransferId(Long transferId);
}