package com.itplamen.digitalwallet.paymentservice.repository;

import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface PaymentRepository extends ReactiveCrudRepository<Payment, UUID> { }
