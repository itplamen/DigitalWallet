package com.itplamen.digitalwallet.transfer.repository;

import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface TransferRepository extends ReactiveCrudRepository<Transfer, Long> {

    @Query("SELECT * FROM transfers WHERE idempotency_key = :idempotencyKey")
    Mono<Transfer> findByIdempotencyKey(String idempotencyKey);

    @Query("SELECT * FROM transfers WHERE status = :status")
    Flux<Transfer> findByStatus(TransferStatus status);
}