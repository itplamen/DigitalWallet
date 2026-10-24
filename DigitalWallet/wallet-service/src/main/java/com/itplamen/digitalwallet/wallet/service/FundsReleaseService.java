package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.Reservation;
import com.itplamen.digitalwallet.wallet.entity.ReservationStatus;
import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.event.FundsReleasedEvent;
import com.itplamen.digitalwallet.wallet.event.PayoutFailedEvent;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.ReservationRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FundsReleaseService {

    private static final String CONSUMER = "wallet-payout-failed";

    private final WalletRepository walletRepository;
    private final WalletEventStore walletEventStore;
    private final WalletOutboxService walletOutboxService;
    private final InboxMessageRepository inboxMessageRepository;
    private final ReservationRepository reservationRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> releaseFunds(UUID eventId, PayoutFailedEvent payload) {
        return Mono.defer(() -> {
                    if (payload == null || payload.paymentId() == null) {
                        return Mono.error(new IllegalArgumentException("PayoutFailed event has no paymentId"));
                    }

                    return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                            .flatMap(inserted -> inserted == 0
                                    ? Mono.empty()
                                    : reservationRepository.findByPaymentId(payload.paymentId())
                                            .filter(reservation -> reservation.getStatus() == ReservationStatus.ACTIVE)
                                            .flatMap(this::release));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> release(Reservation reservation) {
        return walletRepository.getById(reservation.getWalletId())
                .flatMap(wallet -> releaseToWallet(wallet, reservation));
    }

    private Mono<Void> releaseToWallet(Wallet wallet, Reservation reservation) {
        Instant now = Instant.now();
        FundsReleasedEvent event = new FundsReleasedEvent(
                reservation.getPaymentId(), wallet.getId(), wallet.getAccountId(), reservation.getAmount(), wallet.getCurrency());

        return walletEventStore.apply(wallet, "FundsReleased", event, reservation.getAmount(), now)
                .then(reservationRepository.complete(reservation.getPaymentId(), ReservationStatus.RELEASED, now))
                .flatMap(updatedRows -> updatedRows == 1
                        ? Mono.empty()
                        : Mono.error(new IllegalStateException(
                                "Reservation was not active for payment " + reservation.getPaymentId())))
                .then(walletOutboxService.save(reservation.getPaymentId(), "FundsReleased", event, now));
    }
}
