package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.entity.OutboxMessage;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
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
        try {
            OutboxMessage message = OutboxMessage.builder()
                    .aggregateType("Payment")
                    .aggregateId(payment.getId().toString())
                    .eventType("WithdrawalRequested")
                    .payload(Json.of(objectMapper.writeValueAsString(payment)))
                    .createdAt(Instant.now())
                    .build();

            return outboxMessageRepository.save(message).then();
        } catch (JacksonException error) {
            return Mono.error(new IllegalStateException("Failed to serialize WithdrawalRequested event for payment " + payment.getId(), error));
        }
    }
}
