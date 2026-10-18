package com.itplamen.digitalwallet.transfer.service;

import com.itplamen.digitalwallet.transfer.dto.TransferInitiationResult;
import com.itplamen.digitalwallet.transfer.dto.TransferRequest;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
import com.itplamen.digitalwallet.transfer.exception.InvalidTransferStatusTransitionException;
import com.itplamen.digitalwallet.transfer.exception.TransferNotFoundException;
import com.itplamen.digitalwallet.transfer.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.transfer.repository.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferService {

    private final TransferRepository transferRepository;
    private final TransferAuditService transferAuditService;
    private final TransferEventOutboxService transferEventOutboxService;
    private final TransferIdempotencyService transferIdempotencyService;
    private final InboxMessageRepository inboxMessageRepository;
    private final TransactionalOperator transactionalOperator;

    private static final String WALLET_SETTLED_CONSUMER = "transfer-wallet-settled";
    private static final String ACCOUNTS_REJECTED_CONSUMER = "transfer-accounts-rejected";

    public Mono<TransferInitiationResult> initiateTransfer(String idempotencyKey, TransferRequest request) {
        return transferIdempotencyService.execute(idempotencyKey, request, this::createTransfer);
    }

    public Mono<Transfer> getTransfer(UUID transferId) {
        return transferRepository.findById(transferId)
                .switchIfEmpty(Mono.error(new TransferNotFoundException(transferId)));
    }

    public Mono<Transfer> transitionStatus(UUID transferId, TransferStatus targetStatus, String reason) {
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

    public Mono<Void> processWalletTransferSettled(UUID eventId, UUID transferId) {
        return inboxMessageRepository.insert(eventId, WALLET_SETTLED_CONSUMER, Instant.now())
                .flatMap(inserted -> {
                    if (inserted == 0) {
                        return Mono.empty();
                    }
                    return transferRepository.findById(transferId)
                            .switchIfEmpty(Mono.error(new TransferNotFoundException(transferId)))
                            .flatMap(transfer -> {
                                if (transfer.getStatus() == TransferStatus.COMPLETED) {
                                    return Mono.empty();
                                }
                                if (!isAllowedTransition(transfer.getStatus(), TransferStatus.COMPLETED)) {
                                    return Mono.error(new InvalidTransferStatusTransitionException(
                                            transferId, transfer.getStatus(), TransferStatus.COMPLETED));
                                }

                                Instant now = Instant.now();
                                TransferStatus previousStatus = transfer.getStatus();
                                transfer.setStatus(TransferStatus.COMPLETED);
                                transfer.setUpdatedAt(now);
                                return transferRepository.save(transfer)
                                        .flatMap(saved -> transferAuditService.recordStatusTransition(
                                                        transferId,
                                                        previousStatus,
                                                        TransferStatus.COMPLETED,
                                                        "WalletTransferSettled",
                                                        now)
                                                .then(transferEventOutboxService.saveCompleted(saved)));
                            });
                })
                .as(transactionalOperator::transactional);
    }

    public Mono<Void> processAccountsRejected(UUID eventId, UUID transferId, String reason) {
        return inboxMessageRepository.insert(eventId, ACCOUNTS_REJECTED_CONSUMER, Instant.now())
                .flatMap(inserted -> {
                    if (inserted == 0) {
                        return Mono.empty();
                    }

                    return transferRepository.findById(transferId)
                            .switchIfEmpty(Mono.error(new TransferNotFoundException(transferId)))
                            .flatMap(transfer -> {
                                if (transfer.getStatus() == TransferStatus.FAILED) {
                                    return Mono.empty();
                                }
                                if (!isAllowedTransition(transfer.getStatus(), TransferStatus.FAILED)) {
                                    return Mono.error(new InvalidTransferStatusTransitionException(transferId, transfer.getStatus(), TransferStatus.FAILED));
                                }

                                Instant now = Instant.now();
                                TransferStatus previousStatus = transfer.getStatus();
                                transfer.setStatus(TransferStatus.FAILED);
                                transfer.setUpdatedAt(now);

                                return transferRepository.save(transfer)
                                        .flatMap(saved -> transferAuditService.recordStatusTransition(
                                                        transferId,
                                                        previousStatus,
                                                        TransferStatus.FAILED,
                                                        reason,
                                                        now)
                                                .then(transferEventOutboxService.saveFailed(saved, reason)));
                            });
                })
                .as(transactionalOperator::transactional);
    }

    private boolean isAllowedTransition(TransferStatus current, TransferStatus target) {
        if (current == null || target == null) {
            return false;
        }
        return switch (current) {
            case PENDING -> Set.of(
                    TransferStatus.COMPLETED,
                    TransferStatus.FAILED).contains(target);
            case COMPLETED, FAILED -> false;
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
                .sourceAccountId(request.sourceAccountId())
                .targetAccountId(request.targetAccountId())
                .amount(request.amount())
                .currency(request.currency())
                .status(TransferStatus.PENDING)
                .createdAt(Instant.now())
                .build();
    }
}