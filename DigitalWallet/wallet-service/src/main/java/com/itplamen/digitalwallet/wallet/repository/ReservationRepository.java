package com.itplamen.digitalwallet.wallet.repository;

import com.itplamen.digitalwallet.wallet.entity.Reservation;
import com.itplamen.digitalwallet.wallet.entity.ReservationStatus;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Repository
public interface ReservationRepository extends ReactiveCrudRepository<Reservation, UUID> {

    Mono<Reservation> findByPaymentId(UUID paymentId);

    @Modifying
    @Query("""
            UPDATE reservations
            SET status = :status, updated_at = :updatedAt
            WHERE payment_id = :paymentId AND status = 'ACTIVE'
            """)
    Mono<Integer> complete(
            @Param("paymentId") UUID paymentId,
            @Param("status") ReservationStatus status,
            @Param("updatedAt") Instant updatedAt);
}
