package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.event.DepositWalletRejectedEvent;
import com.itplamen.digitalwallet.wallet.event.DepositWalletValidatedEvent;
import com.itplamen.digitalwallet.wallet.event.PaymentAccountValidatedEvent;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
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
                            .flatMap(inserted -> inserted == 0 ? Mono.empty() : checkWallet(payload));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> checkWallet(PaymentAccountValidatedEvent payload) {
        if (!CurrencyCodes.isValid(payload.currency())) {
            return reject(payload, "INVALID_CURRENCY");
        }

        return walletRepository.findByAccountIdAndCurrency(payload.accountId(), CurrencyCodes.normalize(payload.currency()))
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(wallet -> wallet
                        .map(found -> confirm(payload, found))
                        .orElseGet(() -> rejectMissingWallet(payload)));
    }

    private Mono<Void> rejectMissingWallet(PaymentAccountValidatedEvent payload) {
        return walletRepository.existsByAccountId(payload.accountId())
                .flatMap(hasWallets -> reject(payload, hasWallets ? "CURRENCY_MISMATCH" : "WALLET_NOT_FOUND"));
    }

    private Mono<Void> confirm(PaymentAccountValidatedEvent payload, Wallet wallet) {
        DepositWalletValidatedEvent event = new DepositWalletValidatedEvent(
                payload.paymentId(), wallet.getId(), wallet.getAccountId(), payload.amount(), wallet.getCurrency());

        return walletOutboxService.save(payload.paymentId(), "DepositWalletValidated", event, Instant.now());
    }

    private Mono<Void> reject(PaymentAccountValidatedEvent payload, String reason) {
        log.warn("Wallet validation for deposit {} failed: {}", payload.paymentId(), reason);
        return walletOutboxService.save(
                payload.paymentId(),
                "DepositWalletRejected",
                new DepositWalletRejectedEvent(payload.paymentId(), reason),
                Instant.now());
    }

    private void validate(PaymentAccountValidatedEvent payload) {
        if (payload == null || payload.paymentId() == null || payload.accountId() == null) {
            throw new IllegalArgumentException("DepositAccountValidated event has no payment or account ID");
        }
    }
}
