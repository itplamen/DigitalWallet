package com.itplamen.digitalwallet.wallet.repository;

import com.itplamen.digitalwallet.wallet.entity.Transfer;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransferRepository extends ReactiveCrudRepository<Transfer, Long> {}
