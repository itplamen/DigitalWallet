package com.itplamen.digitalwallet.transfer.service;

import com.itplamen.digitalwallet.transfer.dto.TransferInitiationResult;
import com.itplamen.digitalwallet.transfer.dto.TransferRequest;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
import com.itplamen.digitalwallet.transfer.entity.TransferType;
import com.itplamen.digitalwallet.transfer.exception.IdempotencyConflictException;
import com.itplamen.digitalwallet.transfer.exception.InvalidTransferStatusTransitionException;
import com.itplamen.digitalwallet.transfer.exception.TransferNotFoundException;
import com.itplamen.digitalwallet.transfer.exception.UnsupportedTransferTypeException;
import com.itplamen.digitalwallet.transfer.repository.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Locale;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferService {

    private final TransferRepository transferRepository;
    private final TransferAuditService transferAuditService;
    private final TransferEventOutboxService transferEventOutboxService;
    private final TransactionalOperator transactionalOperator;

    public Mono<TransferInitiationResult> initiateTransfer(TransferRequest request) {
        TransferRequest normalizedRequest = normalizeRequest(request);
        String idempotencyKey = normalizedRequest.idempotencyKey();

        return transferRepository.findByIdempotencyKey(idempotencyKey)
                .flatMap(existing -> resolveIdempotency(existing, normalizedRequest))
                .switchIfEmpty(Mono.defer(() -> createTransfer(normalizedRequest)
                        .as(transactionalOperator::transactional)
                        .map(transfer -> new TransferInitiationResult(transfer, true))
                        .onErrorResume(DataIntegrityViolationException.class, error ->
                                transferRepository.findByIdempotencyKey(idempotencyKey)
                                        .flatMap(existing -> resolveIdempotency(existing, normalizedRequest))
                                        .switchIfEmpty(Mono.error(error)))));
    }

    public Mono<Transfer> getTransfer(Long transferId) {
        return transferRepository.findById(transferId)
                .switchIfEmpty(Mono.error(new TransferNotFoundException(transferId)));
    }

    public Mono<Transfer> transitionStatus(Long transferId, TransferStatus targetStatus, String reason) {
        return transferRepository.findById(transferId)
                .switchIfEmpty(Mono.error(new TransferNotFoundException(transferId)))
                .flatMap(transfer -> {
                    TransferStatus currentStatus = transfer.getStatus();
                    if (currentStatus == targetStatus) {
                        log.info("Transfer {} already has status {}", transferId, targetStatus);
                        return Mono.empty();
                    }
                    if (!isAllowedTransition(currentStatus, targetStatus)) {
                        return Mono.error(new InvalidTransferStatusTransitionException(
                                transferId, currentStatus, targetStatus));
                    }

                    Instant now = Instant.now();
                    transfer.setStatus(targetStatus);
                    transfer.setUpdatedAt(now);
                    return transferRepository.save(transfer)
                            .flatMap(saved -> transferAuditService
                                    .recordStatusTransition(
                                            transferId, currentStatus, targetStatus, reason, now)
                                    .thenReturn(saved));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<TransferInitiationResult> resolveIdempotency(
            Transfer existing,
            TransferRequest request) {
        if (!matches(existing, request)) {
            return Mono.error(new IdempotencyConflictException(request.idempotencyKey()));
        }
        log.info("Idempotency hit for key: {}", request.idempotencyKey());
        return Mono.just(new TransferInitiationResult(existing, false));
    }

    private boolean matches(Transfer transfer, TransferRequest request) {
        return transfer.getSourceWalletId().equals(request.sourceWalletId())
                && transfer.getTargetWalletId().equals(request.targetWalletId())
                && transfer.getAmount().compareTo(request.amount()) == 0
                && transfer.getCurrency().equals(request.currency())
                && transfer.getType() == request.type();
    }

    private TransferRequest normalizeRequest(TransferRequest request) {
        if (request.type() != TransferType.P2P) {
            throw new UnsupportedTransferTypeException(request.type());
        }
        return new TransferRequest(
                request.sourceWalletId(),
                request.targetWalletId(),
                request.amount(),
                request.currency().toUpperCase(Locale.ROOT),
                request.type(),
                request.idempotencyKey().strip());
    }

    private boolean isAllowedTransition(TransferStatus current, TransferStatus target) {
        if (current == null || target == null) {
            return false;
        }
        return switch (current) {
            case PENDING -> Set.of(
                    TransferStatus.SOURCE_DEBITED,
                    TransferStatus.COMPLETED,
                    TransferStatus.FAILED).contains(target);
            case SOURCE_DEBITED -> Set.of(
                    TransferStatus.COMPLETED,
                    TransferStatus.FAILED,
                    TransferStatus.REVERSED).contains(target);
            case COMPLETED, FAILED, REVERSED -> false;
        };
    }

    private Mono<Transfer> createTransfer(TransferRequest request) {
        Transfer newTransfer = buildTransfer(request);

        return transferRepository.save(newTransfer)
                .flatMap(savedTransfer -> {
                    return transferAuditService.recordInitiated(savedTransfer)
                            .then(transferEventOutboxService.saveInitiated(savedTransfer))
                            .thenReturn(savedTransfer);
                })
                .doOnSuccess(transfer -> log.info("Successfully initiated transfer ID: {}", transfer.getId()))
                .doOnError(error -> log.error("Failed to initiate transfer", error));
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
}