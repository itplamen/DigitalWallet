package com.itplamen.digitalwallet.transfer.service;

import com.itplamen.digitalwallet.transfer.entity.AuditLog;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
import com.itplamen.digitalwallet.transfer.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TransferAuditService {

    private final AuditLogRepository auditLogRepository;

    public Mono<Void> recordInitiated(Transfer transfer) {
        return recordStatusTransition(transfer.getId(), null, TransferStatus.PENDING, "Transfer initiated", Instant.now());
    }

    public Mono<Void> recordStatusTransition(
            UUID transferId,
            TransferStatus fromStatus,
            TransferStatus toStatus,
            String reason,
            Instant createdAt) {
        AuditLog auditLog = AuditLog.builder()
                .transferId(transferId)
                .fromStatus(fromStatus)
                .toStatus(toStatus)
                .reason(reason)
                .createdAt(createdAt)
                .build();

        return auditLogRepository.save(auditLog).then();
    }
}
