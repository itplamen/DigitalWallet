package com.itplamen.digitalwallet.wallet.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.wallet.entity.OutboxMessage;
import com.itplamen.digitalwallet.wallet.entity.Reservation;
import com.itplamen.digitalwallet.wallet.entity.ReservationStatus;
import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletService {

    private final ObjectMapper objectMapper;
    private final WalletRepository walletRepository;
    private final InboxMessageRepository inboxMessageRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxMessageRepository outboxRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> processAccountsValidated(UUID eventId, TransferEventPayload payload) {
        return Mono.defer(() -> {
                    validate(payload);
                    return inboxMessageRepository.insert(eventId, "wallet-accounts-validated", Instant.now())
                            .flatMap(inserted -> inserted == 0
                                    ? Mono.empty()
                                    : reservationRepository.findByPaymentId(payload.id())
                                            .hasElement()
                                            .flatMap(alreadyProcessed -> alreadyProcessed
                                                    ? Mono.empty()
                                                    : settleTransfer(payload)));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> settleTransfer(TransferEventPayload payload) {
        if (payload.sourceAccountId().equals(payload.targetAccountId())) {
            return recordNotSettled(payload.id(), "SOURCE_AND_TARGET_ACCOUNTS_MUST_DIFFER");
        }

        String currency = normalizeCurrency(payload.currency());
        return walletRepository.findByTransferAccounts(payload.sourceAccountId(), payload.targetAccountId())
                .collectList()
                .flatMap(wallets -> {
                    Map<UUID, List<Wallet>> walletsByAccount = wallets.stream().collect(Collectors.groupingBy(Wallet::getAccountId));

                    Wallet sourceWallet = findWallet(walletsByAccount, payload.sourceAccountId(), currency);
                    if (sourceWallet == null) {
                        return recordNotSettled(payload.id(), walletRejectionReason(walletsByAccount, payload.sourceAccountId(), "SOURCE"));
                    }

                    Wallet targetWallet = findWallet(walletsByAccount, payload.targetAccountId(), currency);
                    if (targetWallet == null) {
                        return recordNotSettled(payload.id(), walletRejectionReason(walletsByAccount, payload.targetAccountId(), "TARGET"));
                    }

                    return postTransfer(payload, sourceWallet.getId(), targetWallet.getId());
                });
    }

    private Wallet findWallet(Map<UUID, List<Wallet>> walletsByAccount, UUID accountId, String currency) {
        return walletsByAccount.getOrDefault(accountId, List.of()).stream()
                .filter(wallet -> currency.equals(wallet.getCurrency()))
                .findFirst()
                .orElse(null);
    }

    private String walletRejectionReason(Map<UUID, List<Wallet>> walletsByAccount, UUID accountId, String role) {
        List<Wallet> wallets = walletsByAccount.getOrDefault(accountId, List.of());
        if (wallets.isEmpty()) {
            return role + "_WALLET_NOT_FOUND";
        }

        return role + "_WALLET_CURRENCY_MISMATCH";
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
            UUID targetWalletId) {
        Instant now = Instant.now();
        String currency = normalizeCurrency(payload.currency());
        return walletRepository.debitIfSufficientFunds(
                        sourceWalletId, currency, payload.amount(), now)
                .flatMap(updatedRows -> {
                    if (updatedRows == 0) {
                        return recordNotSettled(payload.id(), "INSUFFICIENT_FUNDS");
                    }
                    return createReservation(sourceWalletId, payload)
                            .flatMap(reservation -> walletRepository.credit(
                                            targetWalletId, currency, payload.amount(), now)
                                    .flatMap(creditedRows -> {
                                        if (creditedRows != 1) {
                                            return Mono.error(new IllegalStateException(
                                                    "Target wallet changed during transfer settlement: " + targetWalletId));
                                        }
                                        return completeReservation(reservation, ReservationStatus.CAPTURED)
                                                .then(saveBalanceEvent(
                                                        payload, sourceWalletId, "WalletDebited", now))
                                                .then(saveBalanceEvent(
                                                        payload, targetWalletId, "WalletCredited", now))
                                                .then(recordResult(
                                                        payload.id(), "COMPLETED", null, "WalletTransferSettled"));
                                    }));
                });
    }

    private Mono<Void> recordNotSettled(UUID transferId, String reason) {
        log.warn("Transfer {} failed in wallet settlement: {}", transferId, reason);
        return recordResult(transferId, "NOT_SETTLED", reason, "WalletTransferNotSettled");
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

    private Mono<Void> saveBalanceEvent(
            TransferEventPayload payload, UUID walletId, String eventType, Instant createdAt) {
        return saveOutbox(payload.id(), eventType,
                new WalletBalanceEvent(payload.id(), walletId, payload.amount(), normalizeCurrency(payload.currency())),
                createdAt);
    }

    private Mono<Void> saveOutbox(
            UUID transferId,
            String eventType,
            Object eventPayload,
            Instant createdAt) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(eventPayload);
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
            String currency) {}

    public record WalletTransferOutcome(UUID transferId, String status, String reason) {}

    public record WalletBalanceEvent(UUID transferId, UUID walletId, BigDecimal amount, String currency) {}
}