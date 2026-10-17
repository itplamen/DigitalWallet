package com.itplamen.digitalwallet.wallet.repository;

import com.itplamen.digitalwallet.wallet.entity.Wallet;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Repository
public interface WalletRepository extends ReactiveCrudRepository<Wallet, UUID> {

    Mono<Wallet> findByAccountIdAndCurrency(UUID accountId, String currency);

    @Modifying
    @Query("""
            UPDATE wallets
            SET balance = balance - :amount, updated_at = :updatedAt, version = version + 1
            WHERE id = :walletId AND currency = :currency AND balance >= :amount
            """)
    Mono<Integer> debitIfSufficientFunds(
            @Param("walletId") UUID walletId,
            @Param("currency") String currency,
            @Param("amount") BigDecimal amount,
            @Param("updatedAt") Instant updatedAt);

    @Modifying
    @Query("""
            UPDATE wallets
            SET balance = balance + :amount, updated_at = :updatedAt, version = version + 1
            WHERE id = :walletId AND currency = :currency
            """)
    Mono<Integer> credit(
            @Param("walletId") UUID walletId,
            @Param("currency") String currency,
            @Param("amount") BigDecimal amount,
            @Param("updatedAt") Instant updatedAt);
}