package com.itplamen.digitalwallet.account.repository;

import com.itplamen.digitalwallet.account.entity.Account;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface AccountRepository extends R2dbcRepository<Account, UUID> {

    @Query("SELECT * FROM accounts WHERE account_number = :accountNumber")
    Mono<Account> findByAccountNumber(String accountNumber);

    @Query("SELECT * FROM accounts WHERE id IN (:sourceAccountId, :targetAccountId)")
    Flux<Account> findByIds(
            @Param("sourceAccountId") UUID sourceAccountId,
            @Param("targetAccountId") UUID targetAccountId);
}