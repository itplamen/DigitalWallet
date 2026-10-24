package com.itplamen.digitalwallet.wallet.service;

import com.itplamen.digitalwallet.wallet.entity.OutboxMessage;
import com.itplamen.digitalwallet.wallet.entity.Wallet;
import com.itplamen.digitalwallet.wallet.event.FundsReservedEvent;
import com.itplamen.digitalwallet.wallet.event.WithdrawalAccountValidatedEvent;
import com.itplamen.digitalwallet.wallet.repository.InboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.OutboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.ReservationRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalReservationService {

    private static final String CONSUMER = "wallet-withdrawal-account-validated";

    private final ObjectMapper objectMapper;
    private final WalletRepository walletRepository;
    private final WalletEventStore walletEventStore;
    private final InboxMessageRepository inboxMessageRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxMessageRepository outboxRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> reserveFunds(UUID eventId, WithdrawalAccountValidatedEvent payload) {
        return Mono.defer(() -> {
                    validate(payload);
                    return inboxMessageRepository.insert(eventId, CONSUMER, Instant.now())
                            .flatMap(inserted -> inserted == 0
                                    ? Mono.empty()
                                    : reservationRepository.findByPaymentId(payload.paymentId())
                                            .hasElement()
                                            .flatMap(alreadyReserved -> alreadyReserved
                                                    ? Mono.empty()
                                                    : reserve(payload)));
                })
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> reserve(WithdrawalAccountValidatedEvent payload) {
        String currency = payload.currency().toUpperCase(Locale.ROOT);
        return walletRepository.findByAccountId(payload.accountId())
                .filter(wallet -> currency.equals(wallet.getCurrency()))
                .filter(wallet -> wallet.getBalance().compareTo(payload.amount()) >= 0)
                .next()
                .flatMap(wallet -> holdFunds(payload, wallet, currency));
    }

    private Mono<Void> holdFunds(WithdrawalAccountValidatedEvent payload, Wallet wallet, String currency) {
        Instant now = Instant.now();
        FundsReservedEvent event = new FundsReservedEvent(
                payload.paymentId(), wallet.getId(), wallet.getAccountId(), payload.amount(), currency);

        return walletEventStore.apply(wallet, "FundsReserved", event, payload.amount().negate(), now)
                .then(reservationRepository.insertActive(
                        UUID.randomUUID(), wallet.getId(), payload.paymentId(), payload.amount(), now))
                .then(saveOutbox(payload.paymentId(), "FundsReserved", event, now));
    }

    private Mono<Void> saveOutbox(UUID paymentId, String eventType, Object eventPayload, Instant createdAt) {
        try {
            OutboxMessage outbox = OutboxMessage.builder()
                    .aggregateType("WALLET")
                    .aggregateId(paymentId.toString())
                    .eventType(eventType)
                    .payload(Json.of(objectMapper.writeValueAsString(eventPayload)))
                    .createdAt(createdAt)
                    .build();

            return outboxRepository.save(outbox).then();
        } catch (JacksonException e) {
            log.error("Failed to serialize outbox payload for payment ID: {}", paymentId, e);
            return Mono.error(e);
        }
    }

    private void validate(WithdrawalAccountValidatedEvent payload) {
        if (payload == null
                || payload.paymentId() == null
                || payload.accountId() == null
                || payload.amount() == null
                || payload.amount().signum() <= 0
                || payload.currency() == null
                || !payload.currency().matches("[A-Za-z]{3}")) {
            throw new IllegalArgumentException("WithdrawalAccountValidated event has invalid reservation fields");
        }

        BigDecimal normalizedAmount = payload.amount().stripTrailingZeros();
        if (normalizedAmount.scale() > 8 || normalizedAmount.precision() - normalizedAmount.scale() > 11) {
            throw new IllegalArgumentException("Withdrawal amount exceeds the supported precision");
        }
    }
}
