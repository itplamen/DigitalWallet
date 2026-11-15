package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PendingPaymentFailureService {

    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<Void> fail(UUID paymentId, String reason) {
        return paymentRepository.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.PENDING)
                .flatMap(payment -> markFailed(payment, reason))
                .flatMap(paymentEventOutboxService::saveWithdrawalFailed);
    }

    private Mono<Payment> markFailed(Payment payment, String reason) {
        payment.setStatus(PaymentStatus.FAILED);
        payment.setReason(reason);
        payment.setUpdatedAt(Instant.now());

        return paymentRepository.save(payment);
    }
}
