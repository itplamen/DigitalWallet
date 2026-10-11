package com.itplamen.digitalwallet.transfer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itplamen.digitalwallet.transfer.dto.TransferRequest;
import com.itplamen.digitalwallet.transfer.entity.AuditLog;
import com.itplamen.digitalwallet.transfer.entity.OutboxMessage;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
import com.itplamen.digitalwallet.transfer.repository.AuditLogRepository;
import com.itplamen.digitalwallet.transfer.repository.OutboxMessageRepository;
import com.itplamen.digitalwallet.transfer.repository.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferService {

    private final ObjectMapper objectMapper;
    private final TransferRepository transferRepository;
    private final AuditLogRepository auditLogRepository;
    private final OutboxMessageRepository outboxEventRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Transfer> initiateTransfer(TransferRequest request) {
        return transferRepository.findByIdempotencyKey(request.idempotencyKey())
                .doOnNext(existing -> log.info("Idempotency hit for key: {}", request.idempotencyKey()))
                .switchIfEmpty(Mono.defer(() -> executeTransferTransaction(request)));
    }

    private Mono<Transfer> executeTransferTransaction(TransferRequest request) {
        Transfer newTransfer = buildTransfer(request);

        return transferRepository.save(newTransfer)
                .flatMap(savedTransfer -> {
                    AuditLog auditLog = buildAuditLog(savedTransfer);
                    OutboxMessage outboxEvent = buildOutboxEvent(savedTransfer);

                    return auditLogRepository.save(auditLog)
                            .then(outboxEventRepository.save(outboxEvent))
                            .thenReturn(savedTransfer);
                })
                .as(transactionalOperator::transactional)
                .doOnSuccess(transfer -> log.info("Successfully initiated transfer ID: {}", transfer.getId()))
                .doOnError(error -> log.error("Failed to initiate transfer: {}", error.getMessage()));
    }

    private Transfer buildTransfer(TransferRequest request) {
        return Transfer.builder()
                .sourceWalletId(request.sourceWalletId())
                .targetWalletId(request.targetWalletId())
                .amount(request.amount())
                .currency(request.currency())
                .type(request.type())
                .status(TransferStatus.PENDING)
                .idempotencyKey(request.idempotencyKey())
                .createdAt(Instant.now())
                .build();
    }

    private AuditLog buildAuditLog(Transfer transfer) {
        return AuditLog.builder()
                .transferId(transfer.getId())
                .fromStatus(null)
                .toStatus(TransferStatus.PENDING)
                .reason("Transfer initiated")
                .createdAt(Instant.now())
                .build();
    }

    private OutboxMessage buildOutboxEvent(Transfer transfer) {
        try {
            String payload = objectMapper.writeValueAsString(transfer);

            return OutboxMessage.builder()
                    .aggregateType("Transfer")
                    .aggregateId(String.valueOf(transfer.getId()))
                    .eventType("TRANSFER_INITIATED")
                    .payload(payload)
                    .processed(false)
                    .createdAt(Instant.now())
                    .build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize transfer to outbox payload", e);
        }
    }
}