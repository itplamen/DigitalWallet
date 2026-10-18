package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.repository.WalletEventRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class WalletEventStore {

    private final WalletEventRepository walletEventRepository;
    private final WalletRepository walletRepository;
    private final ObjectMapper objectMapper;

    public Mono<Void> apply(
            Wallet wallet,
            String eventType,
            Object eventPayload,
            BigDecimal balanceDelta,
            Instant occurredAt) {
        BigDecimal newBalance = wallet.getBalance().add(balanceDelta);
        if (newBalance.signum() < 0) {
            return Mono.error(new IllegalStateException("Wallet event would create a negative balance"));
        }

        long currentVersion = wallet.getVersion() == null ? 0 : wallet.getVersion();
        long nextVersion = currentVersion + 1;
        UUID eventId = UUID.randomUUID();

        return serialize(eventPayload)
                .flatMap(payload -> walletEventRepository.append(wallet.getId(), nextVersion, eventId, eventType, Json.of(payload), occurredAt)
                        .flatMap(rows -> rows == 1L
                                ? Mono.empty()
                                : Mono.error(new IllegalStateException("Wallet event append affected " + rows + " rows"))))
                .then(walletRepository.updateBalanceProjection(
                        wallet.getId(),
                        wallet.getCurrency(),
                        newBalance,
                        balanceDelta,
                        currentVersion,
                        occurredAt))
                .flatMap(updatedRows -> updatedRows == 1
                        ? Mono.empty()
                        : Mono.error(new IllegalStateException("Concurrent wallet update detected for " + wallet.getId())));
    }

    private Mono<String> serialize(Object eventPayload) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(eventPayload))
                .onErrorMap(JacksonException.class,
                        error -> new IllegalStateException("Failed to serialize wallet domain event", error));
    }
}
