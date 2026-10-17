package com.itplamen.digitalwallet.transfer.repository;

import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.entity.TransferStatus;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import java.util.UUID;

@Repository
public interface TransferRepository extends ReactiveCrudRepository<Transfer, UUID> {

    Flux<Transfer> findByStatus(TransferStatus status);
}