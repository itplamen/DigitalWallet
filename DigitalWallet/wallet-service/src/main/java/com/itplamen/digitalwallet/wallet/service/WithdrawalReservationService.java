package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.event.FundsReservationFailedEvent;
import com.itplamen.digitalwallet.wallet.event.FundsReservedEvent;
import com.itplamen.digitalwallet.wallet.event.PaymentAccountValidatedEvent;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.ReservationRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalReservationService {

    private static final String CONSUMER = "wallet-withdrawal-account-validated";
    private static final Pattern CURRENCY_PATTERN = Pattern.compile("[A-Za-z]{3}");

    private final WalletRepository walletRepository;
    private final WalletEventStore walletEventStore;
    private final WalletOutboxService walletOutboxService;
    private final InboxMessageRepository inboxMessageRepository;
    private final ReservationRepository reservationRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> reserveFunds(UUID eventId, PaymentAccountValidatedEvent payload) {
        return Mono.defer(() -> {
                    validate(payload);
                    return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                            .flatMap(inserted -> inserted == 0
                                    ? Mono.empty()
                                    : reservationRepository.findByPaymentId(payload.paymentId())
                                            .hasElement()
                                            .flatMap(alreadyReserved -> alreadyReserved
                                                    ? Mono.empty()
                                                    : reserve(payload)));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> reserve(PaymentAccountValidatedEvent payload) {
        if (payload.currency() == null || !CURRENCY_PATTERN.matcher(payload.currency()).matches()) {
            return rejectReservation(payload, "INVALID_CURRENCY");
        }

        String currency = payload.currency().toUpperCase(Locale.ROOT);
        return walletRepository.findByAccountId(payload.accountId())
                .collectList()
                .flatMap(wallets -> {
                    if (wallets.isEmpty()) {
                        return rejectReservation(payload, "WALLET_NOT_FOUND");
                    }

                    return wallets.stream()
                            .filter(wallet -> currency.equals(wallet.getCurrency()))
                            .findFirst()
                            .map(wallet -> holdOrReject(payload, wallet, currency))
                            .orElseGet(() -> rejectReservation(payload, "CURRENCY_MISMATCH"));
                });
    }

    private Mono<Void> holdOrReject(PaymentAccountValidatedEvent payload, Wallet wallet, String currency) {
        return wallet.getBalance().compareTo(payload.amount()) >= 0
                ? holdFunds(payload, wallet, currency)
                : rejectReservation(payload, "INSUFFICIENT_FUNDS");
    }

    private Mono<Void> rejectReservation(PaymentAccountValidatedEvent payload, String reason) {
        log.warn("Funds reservation for payment {} failed: {}", payload.paymentId(), reason);
        return walletOutboxService.save(
                payload.paymentId(),
                "FundsReservationFailed",
                new FundsReservationFailedEvent(payload.paymentId(), reason),
                Instant.now());
    }

    private Mono<Void> holdFunds(PaymentAccountValidatedEvent payload, Wallet wallet, String currency) {
        Instant now = Instant.now();
        FundsReservedEvent event = new FundsReservedEvent(
                payload.paymentId(), wallet.getId(), wallet.getAccountId(), payload.amount(), currency);

        return walletEventStore.apply(wallet, "FundsReserved", event, payload.amount().negate(), now)
                .then(reservationRepository.insertActive(
                        UUID.randomUUID(), wallet.getId(), payload.paymentId(), payload.amount(), now))
                .then(walletOutboxService.save(payload.paymentId(), "FundsReserved", event, now));
    }

    private void validate(PaymentAccountValidatedEvent payload) {
        if (payload == null
                || payload.paymentId() == null
                || payload.accountId() == null
                || payload.amount() == null
                || payload.amount().signum() <= 0) {
            throw new IllegalArgumentException("WithdrawalAccountValidated event has invalid reservation fields");
        }

        BigDecimal normalizedAmount = payload.amount().stripTrailingZeros();
        if (normalizedAmount.scale() > 8 || normalizedAmount.precision() - normalizedAmount.scale() > 11) {
            throw new IllegalArgumentException("Withdrawal amount exceeds the supported precision");
        }
    }
}
