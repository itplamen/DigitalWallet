package com.itplamen.digitalwallet.account.repository;

import com.itplamen.digitalwallet.account.entity.Account;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface AccountRepository extends R2dbcRepository<Account, Long> {

    @Query("SELECT * FROM accounts WHERE account_number = :accountNumber")
    Mono<Account> findByAccountNumber(String accountNumber);
}