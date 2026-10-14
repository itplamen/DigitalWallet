package com.itplamen.digitalwallet.wallet.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.wallet.entity.LedgerEntry;
import com.itplamen.digitalwallet.wallet.entity.OutboxMessage;
import com.itplamen.digitalwallet.wallet.entity.Transfer;
import com.itplamen.digitalwallet.wallet.repository.LedgerEntryRepository;
import com.itplamen.digitalwallet.wallet.repository.OutboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import com.itplamen.digitalwallet.wallet.repository.TransferRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletService {

    private final ObjectMapper objectMapper;
    private final WalletRepository walletRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final TransferRepository transferRepository;
    private final OutboxMessageRepository outboxRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> processTransfer(TransferEventPayload payload) {
        return Mono.defer(() -> {
                    validate(payload);
                    return transferRepository.findById(payload.id())
                            .hasElement()
                            .flatMap(alreadyProcessed -> alreadyProcessed
                                    ? Mono.empty()
                                    : settleTransfer(payload));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> settleTransfer(TransferEventPayload payload) {
        if (payload.sourceWalletId().equals(payload.targetWalletId())) {
            return recordFailure(payload.id(), "SOURCE_AND_TARGET_WALLETS_MUST_DIFFER");
        }

        return walletRepository.findById(payload.sourceWalletId())
                .switchIfEmpty(Mono.defer(() -> recordFailure(payload.id(), "SOURCE_WALLET_NOT_FOUND")
                        .then(Mono.empty())))
                .flatMap(sourceWallet -> walletRepository.findById(payload.targetWalletId())
                        .switchIfEmpty(Mono.defer(() -> recordFailure(payload.id(), "TARGET_WALLET_NOT_FOUND")
                                .then(Mono.empty())))
                        .flatMap(targetWallet -> {
                            String currency = normalizeCurrency(payload.currency());
                            if (!currency.equals(sourceWallet.getCurrency())
                                    || !currency.equals(targetWallet.getCurrency())) {
                                return recordFailure(payload.id(), "CURRENCY_MISMATCH");
                            }
                            return postTransfer(payload, sourceWallet.getId(), targetWallet.getId());
                        }));
    }

    private Mono<Void> postTransfer(TransferEventPayload payload, Long sourceWalletId, Long targetWalletId) {
        Instant now = Instant.now();
        String currency = normalizeCurrency(payload.currency());
        return walletRepository.debitIfSufficientFunds(
                        sourceWalletId, currency, payload.amount(), now)
                .flatMap(updatedRows -> {
                    if (updatedRows == 0) {
                        return recordFailure(payload.id(), "INSUFFICIENT_FUNDS");
                    }
                    return walletRepository.credit(targetWalletId, currency, payload.amount(), now)
                            .flatMap(creditedRows -> {
                                if (creditedRows != 1) {
                                    return Mono.error(new IllegalStateException(
                                            "Target wallet changed during transfer settlement: " + targetWalletId));
                                }
                                return saveLedgerEntries(payload, sourceWalletId, targetWalletId, now);
                            });
                });
    }

    private Mono<Void> saveLedgerEntries(
            TransferEventPayload payload,
            Long sourceWalletId,
            Long targetWalletId,
            Instant now) {
        List<LedgerEntry> entries = List.of(
                buildLedgerEntry(sourceWalletId, payload, "DEBIT", now),
                buildLedgerEntry(targetWalletId, payload, "CREDIT", now));

        return ledgerEntryRepository.saveAll(entries)
                .then(recordResult(payload.id(), "COMPLETED", null, "WALLET_TRANSFER_SETTLED"));
    }

    private LedgerEntry buildLedgerEntry(
            Long walletId,
            TransferEventPayload payload,
            String entryType,
            Instant createdAt) {
        return LedgerEntry.builder()
                .walletId(walletId)
                .transferId(payload.id())
                .entryType(entryType)
                .amount(payload.amount())
                .currency(normalizeCurrency(payload.currency()))
                .createdAt(createdAt)
                .build();
    }

    private Mono<Void> recordFailure(Long transferId, String reason) {
        log.warn("Transfer {} failed in wallet settlement: {}", transferId, reason);
        return recordResult(transferId, "FAILED", reason, "WALLET_TRANSFER_FAILED");
    }

    private Mono<Void> recordResult(Long transferId, String status, String reason, String eventType) {
        Instant now = Instant.now();
        Transfer result = Transfer.builder()
                .transferId(transferId)
                .status(status)
                .reason(reason)
                .createdAt(now)
                .build();
        return transferRepository.save(result)
                .then(saveOutbox(transferId, eventType, new WalletTransferOutcome(transferId, status, reason), now));
    }

    private Mono<Void> saveOutbox(
            Long transferId,
            String eventType,
            WalletTransferOutcome outcome,
            Instant createdAt) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(outcome);
            OutboxMessage outbox = OutboxMessage.builder()
                    .aggregateType("WALLET")
                    .aggregateId(transferId.toString())
                    .eventType(eventType)
                    .payload(jsonPayload)
                    .createdAt(createdAt)
                    .processed(false)
                    .version(0L)
                    .build();

            return outboxRepository.save(outbox).then();
        } catch (JacksonException e) {
            log.error("Failed to serialize outbox payload for transfer ID: {}", transferId, e);
            return Mono.error(e);
        }
    }

    private void validate(TransferEventPayload payload) {
        if (payload == null
                || payload.id() == null
                || payload.id() <= 0
                || payload.sourceWalletId() == null
                || payload.sourceWalletId() <= 0
                || payload.targetWalletId() == null
                || payload.targetWalletId() <= 0
                || payload.amount() == null
                || payload.amount().signum() <= 0
                || payload.currency() == null
                || !payload.currency().matches("[A-Za-z]{3}")) {
            throw new IllegalArgumentException("Transfer event has invalid wallet settlement fields");
        }

        BigDecimal normalizedAmount = payload.amount().stripTrailingZeros();
        if (normalizedAmount.scale() > 8 || normalizedAmount.precision() - normalizedAmount.scale() > 11) {
            throw new IllegalArgumentException("Transfer amount exceeds the supported precision");
        }
    }

    private String normalizeCurrency(String currency) {
        return currency.toUpperCase(Locale.ROOT);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TransferEventPayload(
            Long id,
            Long sourceWalletId,
            Long targetWalletId,
            BigDecimal amount,
            String currency
    ) {}

    public record WalletTransferOutcome(Long transferId, String status, String reason) {}
}