package com.itplamen.digitalwallet.wallet.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.wallet.entity.LedgerEntry;
import com.itplamen.digitalwallet.wallet.entity.OutboxMessage;
import com.itplamen.digitalwallet.wallet.entity.Reservation;
import com.itplamen.digitalwallet.wallet.entity.ReservationStatus;
import com.itplamen.digitalwallet.wallet.repository.LedgerEntryRepository;
import com.itplamen.digitalwallet.wallet.repository.OutboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.ReservationRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import io.r2dbc.postgresql.codec.Json;
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
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletService {

    private final ObjectMapper objectMapper;
    private final WalletRepository walletRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxMessageRepository outboxRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> processTransfer(TransferEventPayload payload) {
        return Mono.defer(() -> {
                    validate(payload);
                    return reservationRepository.findByPaymentId(payload.id())
                            .hasElement()
                            .flatMap(alreadyProcessed -> alreadyProcessed
                                    ? Mono.empty()
                                    : settleTransfer(payload));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> settleTransfer(TransferEventPayload payload) {
        if (payload.sourceAccountId().equals(payload.targetAccountId())) {
            return recordFailure(payload.id(), "SOURCE_AND_TARGET_ACCOUNTS_MUST_DIFFER");
        }

        String currency = normalizeCurrency(payload.currency());
        return walletRepository.findByAccountIdAndCurrency(payload.sourceAccountId(), currency)
                .switchIfEmpty(Mono.defer(() -> recordFailure(payload.id(), "SOURCE_WALLET_NOT_FOUND")
                        .then(Mono.empty())))
                .flatMap(sourceWallet -> walletRepository.findByAccountIdAndCurrency(
                                payload.targetAccountId(), currency)
                        .switchIfEmpty(Mono.defer(() -> createReservation(sourceWallet.getId(), payload)
                                .flatMap(reservation -> releaseReservation(
                                        reservation, "TARGET_WALLET_NOT_FOUND"))
                                .then(Mono.empty())))
                        .flatMap(targetWallet -> {
                            if (!currency.equals(sourceWallet.getCurrency())
                                    || !currency.equals(targetWallet.getCurrency())) {
                                return createReservation(sourceWallet.getId(), payload)
                                        .flatMap(reservation -> releaseReservation(reservation, "CURRENCY_MISMATCH"));
                            }
                            return createReservation(sourceWallet.getId(), payload)
                                    .flatMap(reservation -> postTransfer(
                                            payload, sourceWallet.getId(), targetWallet.getId(), reservation));
                        }));
    }

    private Mono<Reservation> createReservation(UUID walletId, TransferEventPayload payload) {
        Instant now = Instant.now();
        Reservation reservation = Reservation.builder()
                .id(UUID.randomUUID())
                .walletId(walletId)
                .paymentId(payload.id())
                .amount(payload.amount())
                .status(ReservationStatus.ACTIVE)
                .createdAt(now)
                .build();
        return reservationRepository.save(reservation);
    }

    private Mono<Void> postTransfer(
            TransferEventPayload payload,
            UUID sourceWalletId,
            UUID targetWalletId,
            Reservation reservation) {
        Instant now = Instant.now();
        String currency = normalizeCurrency(payload.currency());
        return walletRepository.debitIfSufficientFunds(
                        sourceWalletId, currency, payload.amount(), now)
                .flatMap(updatedRows -> {
                    if (updatedRows == 0) {
                        return releaseReservation(reservation, "INSUFFICIENT_FUNDS");
                    }
                    return walletRepository.credit(targetWalletId, currency, payload.amount(), now)
                            .flatMap(creditedRows -> {
                                if (creditedRows != 1) {
                                    return Mono.error(new IllegalStateException(
                                            "Target wallet changed during transfer settlement: " + targetWalletId));
                                }
                                return saveLedgerEntries(payload, sourceWalletId, targetWalletId, reservation, now);
                            });
                });
    }

    private Mono<Void> saveLedgerEntries(
            TransferEventPayload payload,
            UUID sourceWalletId,
            UUID targetWalletId,
            Reservation reservation,
            Instant now) {
        List<LedgerEntry> entries = List.of(
                buildLedgerEntry(sourceWalletId, payload, "DEBIT", now),
                buildLedgerEntry(targetWalletId, payload, "CREDIT", now));

        return ledgerEntryRepository.saveAll(entries)
                .then(completeReservation(reservation, ReservationStatus.CAPTURED))
                .then(recordResult(payload.id(), "COMPLETED", null, "WALLET_TRANSFER_SETTLED"));
    }

    private LedgerEntry buildLedgerEntry(
            UUID walletId,
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

    private Mono<Void> recordFailure(UUID transferId, String reason) {
        log.warn("Transfer {} failed in wallet settlement: {}", transferId, reason);
        return recordResult(transferId, "FAILED", reason, "WALLET_TRANSFER_FAILED");
    }

    private Mono<Void> releaseReservation(Reservation reservation, String reason) {
        return completeReservation(reservation, ReservationStatus.RELEASED)
                .then(recordFailure(reservation.getPaymentId(), reason));
    }

    private Mono<Void> completeReservation(Reservation reservation, ReservationStatus status) {
        return reservationRepository.complete(reservation.getPaymentId(), status, Instant.now())
                .flatMap(updatedRows -> updatedRows == 1
                        ? Mono.empty()
                        : Mono.error(new IllegalStateException(
                                "Reservation was not active for payment " + reservation.getPaymentId())));
    }

    private Mono<Void> recordResult(UUID transferId, String status, String reason, String eventType) {
        Instant now = Instant.now();
        return saveOutbox(transferId, eventType, new WalletTransferOutcome(transferId, status, reason), now);
    }

    private Mono<Void> saveOutbox(
            UUID transferId,
            String eventType,
            WalletTransferOutcome outcome,
            Instant createdAt) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(outcome);
            OutboxMessage outbox = OutboxMessage.builder()
                    .aggregateType("WALLET")
                    .aggregateId(transferId.toString())
                    .eventType(eventType)
                    .payload(Json.of(jsonPayload))
                    .createdAt(createdAt)
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
                || payload.sourceAccountId() == null
                || payload.targetAccountId() == null
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
            UUID id,
            UUID sourceAccountId,
            UUID targetAccountId,
            BigDecimal amount,
            String currency
    ) {}

    public record WalletTransferOutcome(UUID transferId, String status, String reason) {}
}