package com.itplamen.digitalwallet.wallet.repository;

import com.itplamen.digitalwallet.wallet.entity.LedgerEntry;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LedgerEntryRepository extends ReactiveCrudRepository<LedgerEntry, Long> {}
