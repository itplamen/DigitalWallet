package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.event.DepositConfirmedEvent;
import com.itplamen.digitalwallet.wallet.event.WalletCreditedEvent;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DepositCreditService {

    private static final String CONSUMER = "wallet-deposit-confirmed";

    private final WalletRepository walletRepository;
    private final WalletEventStore walletEventStore;
    private final WalletOutboxService walletOutboxService;
    private final InboxMessageRepository inboxMessageRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> creditWallet(UUID eventId, DepositConfirmedEvent payload) {
        return Mono.defer(() -> {
                    validate(payload);
                    return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                            .flatMap(inserted -> inserted == 0
                                    ? Mono.empty()
                                    : findWallet(payload).flatMap(wallet -> credit(payload, wallet)));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Wallet> findWallet(DepositConfirmedEvent payload) {
        String currency = CurrencyCodes.normalize(payload.currency());
        return walletRepository.findByAccountId(payload.accountId())
                .filter(wallet -> currency.equals(wallet.getCurrency()))
                .next()
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "No " + currency + " wallet to credit for payment " + payload.paymentId())));
    }

    private Mono<Void> credit(DepositConfirmedEvent payload, Wallet wallet) {
        Instant now = Instant.now();
        WalletCreditedEvent event = new WalletCreditedEvent(
                payload.paymentId(), wallet.getId(), wallet.getAccountId(), payload.amount(), wallet.getCurrency());

        return walletEventStore.apply(wallet, "WalletCredited", event, payload.amount(), now)
                .then(walletOutboxService.save(payload.paymentId(), "WalletCredited", event, now));
    }

    private void validate(DepositConfirmedEvent payload) {
        if (payload == null
                || payload.paymentId() == null
                || payload.accountId() == null
                || payload.amount() == null
                || payload.amount().signum() <= 0
                || !CurrencyCodes.isValid(payload.currency())) {
            throw new IllegalArgumentException("DepositConfirmed event has invalid credit fields");
        }
    }
}
