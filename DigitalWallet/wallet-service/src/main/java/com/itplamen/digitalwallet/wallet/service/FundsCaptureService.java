package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.Reservation;
import com.itplamen.digitalwallet.wallet.entity.ReservationStatus;
import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.event.FundsCapturedEvent;
import com.itplamen.digitalwallet.wallet.event.PayoutSucceededEvent;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.ReservationRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FundsCaptureService {

    private static final String CONSUMER = "wallet-payout-succeeded";

    private final WalletRepository walletRepository;
    private final WalletEventStore walletEventStore;
    private final WalletOutboxService walletOutboxService;
    private final InboxMessageRepository inboxMessageRepository;
    private final ReservationRepository reservationRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> captureFunds(UUID eventId, PayoutSucceededEvent payload) {
        return Mono.defer(() -> {
                    if (payload == null || payload.paymentId() == null) {
                        return Mono.error(new IllegalArgumentException("PayoutSucceeded event has no paymentId"));
                    }

                    return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                            .flatMap(inserted -> inserted == 0
                                    ? Mono.empty()
                                    : reservationRepository.findByPaymentId(payload.paymentId())
                                            .filter(reservation -> reservation.getStatus() == ReservationStatus.ACTIVE)
                                            .flatMap(this::capture));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> capture(Reservation reservation) {
        return walletRepository.getById(reservation.getWalletId())
                .flatMap(wallet -> captureFromWallet(wallet, reservation));
    }

    private Mono<Void> captureFromWallet(Wallet wallet, Reservation reservation) {
        Instant now = Instant.now();
        FundsCapturedEvent event = new FundsCapturedEvent(
                reservation.getPaymentId(), wallet.getId(), wallet.getAccountId(), reservation.getAmount(), wallet.getCurrency());

        return walletEventStore.apply(wallet, "FundsCaptured", event, BigDecimal.ZERO, now)
                .then(reservationRepository.complete(reservation.getPaymentId(), ReservationStatus.CAPTURED, now))
                .flatMap(updatedRows -> updatedRows == 1
                        ? Mono.empty()
                        : Mono.error(new IllegalStateException(
                                "Reservation was not active for payment " + reservation.getPaymentId())))
                .then(walletOutboxService.save(reservation.getPaymentId(), "FundsCaptured", event, now));
    }
}
