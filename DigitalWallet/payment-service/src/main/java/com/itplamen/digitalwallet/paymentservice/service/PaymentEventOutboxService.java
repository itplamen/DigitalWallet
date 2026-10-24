package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.OutboxMessage;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.event.PayoutFailedEvent;
import com.itplamen.digitalwallet.paymentservice.event.PayoutSucceededEvent;
import com.itplamen.digitalwallet.paymentservice.event.WithdrawalCompletedEvent;
import com.itplamen.digitalwallet.paymentservice.repository.OutboxMessageRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PaymentEventOutboxService {

    private final ObjectMapper objectMapper;
    private final OutboxMessageRepository outboxMessageRepository;

    public Mono<Void> saveWithdrawalRequested(Payment payment) {
        return save(payment, "WithdrawalRequested", payment);
    }

    public Mono<Void> savePayoutSucceeded(Payment payment) {
        return save(payment, "PayoutSucceeded", new PayoutSucceededEvent(
                payment.getId(), payment.getAccountId(), payment.getAmount(), payment.getCurrency()));
    }

    public Mono<Void> savePayoutFailed(Payment payment, String reason) {
        return save(payment, "PayoutFailed", new PayoutFailedEvent(
                payment.getId(), payment.getAccountId(), payment.getAmount(), payment.getCurrency(), reason));
    }

    public Mono<Void> saveWithdrawalCompleted(Payment payment) {
        return save(payment, "WithdrawalCompleted", new WithdrawalCompletedEvent(
                payment.getId(), payment.getAccountId(), payment.getAmount(), payment.getCurrency()));
    }

    private Mono<Void> save(Payment payment, String eventType, Object event) {
        try {
            OutboxMessage message = OutboxMessage.builder()
                    .aggregateType("Payment")
                    .aggregateId(payment.getId().toString())
                    .eventType(eventType)
                    .payload(Json.of(objectMapper.writeValueAsString(event)))
                    .createdAt(Instant.now())
                    .build();

            return outboxMessageRepository.save(message).then();
        } catch (JacksonException error) {
            return Mono.error(new IllegalStateException("Failed to serialize " + eventType + " event for payment " + payment.getId(), error));
        }
    }
}
