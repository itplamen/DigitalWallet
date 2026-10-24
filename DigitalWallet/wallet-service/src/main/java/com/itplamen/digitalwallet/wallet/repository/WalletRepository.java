package com.itplamen.digitalwallet.wallet.repository;

import com.itplamen.digitalwallet.wallet.entity.Wallet;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Repository
public interface WalletRepository extends ReactiveCrudRepository<Wallet, UUID> {

    @Query("""
            SELECT * FROM wallets
            WHERE account_id IN (:sourceAccountId, :targetAccountId)
            ORDER BY id
            FOR UPDATE
            """)
    Flux<Wallet> findByTransferAccounts(
            @Param("sourceAccountId") UUID sourceAccountId,
            @Param("targetAccountId") UUID targetAccountId);

    @Query("""
            SELECT * FROM wallets
            WHERE account_id = :accountId
            ORDER BY id
            FOR UPDATE
            """)
    Flux<Wallet> findByAccountId(@Param("accountId") UUID accountId);

    @Query("SELECT * FROM wallets WHERE id = :walletId FOR UPDATE")
    Mono<Wallet> getById(@Param("walletId") UUID walletId);

    @Modifying
    @Query("""
            UPDATE wallets
            SET balance = :balance, updated_at = :updatedAt, version = version + 1
            WHERE id = :walletId AND currency = :currency
                AND version = :expectedVersion AND balance + :balanceDelta >= 0
            """)
    Mono<Integer> updateBalanceProjection(
            @Param("walletId") UUID walletId,
            @Param("currency") String currency,
            @Param("balance") BigDecimal balance,
            @Param("balanceDelta") BigDecimal balanceDelta,
            @Param("expectedVersion") Long expectedVersion,
            @Param("updatedAt") Instant updatedAt);
}