package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.event.DepositWalletValidatedEvent;
import com.itplamen.digitalwallet.wallet.event.PaymentAccountValidatedEvent;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DepositWalletValidationService {

    private static final String CONSUMER = "wallet-deposit-account-validated";

    private final WalletRepository walletRepository;
    private final WalletOutboxService walletOutboxService;
    private final InboxMessageRepository inboxMessageRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> validateWallet(UUID eventId, PaymentAccountValidatedEvent payload) {
        return Mono.defer(() -> {
                    validate(payload);
                    return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                            .flatMap(inserted -> inserted == 0
                                    ? Mono.empty()
                                    : findWallet(payload).flatMap(wallet -> confirm(payload, wallet)));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Wallet> findWallet(PaymentAccountValidatedEvent payload) {
        return payload.currency() == null
                ? Mono.empty()
                : walletRepository.findByAccountIdAndCurrency(
                        payload.accountId(), payload.currency().toUpperCase(Locale.ROOT));
    }

    private Mono<Void> confirm(PaymentAccountValidatedEvent payload, Wallet wallet) {
        DepositWalletValidatedEvent event = new DepositWalletValidatedEvent(
                payload.paymentId(), wallet.getId(), wallet.getAccountId(), payload.amount(), wallet.getCurrency());

        return walletOutboxService.save(payload.paymentId(), "DepositWalletValidated", event, Instant.now());
    }

    private void validate(PaymentAccountValidatedEvent payload) {
        if (payload == null || payload.paymentId() == null || payload.accountId() == null) {
            throw new IllegalArgumentException("DepositAccountValidated event has no payment or account ID");
        }
    }
}
