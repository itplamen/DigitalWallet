package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.WithdrawalInitiationResult;
import com.itplamen.digitalwallet.paymentservice.dto.WithdrawalRequest;
import com.itplamen.digitalwallet.paymentservice.entity.IdempotencyKey;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentStatus;
import com.itplamen.digitalwallet.paymentservice.entity.PaymentType;
import com.itplamen.digitalwallet.paymentservice.exception.IdempotencyConflictException;
import com.itplamen.digitalwallet.paymentservice.repository.IdempotencyKeyRepository;
import com.itplamen.digitalwallet.paymentservice.repository.PaymentRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final ObjectMapper objectMapper;
    private final TransactionalOperator transactionalOperator;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final PaymentRepository paymentRepository;
    private final PaymentEventOutboxService paymentEventOutboxService;

    public Mono<WithdrawalInitiationResult> createWithdrawal(String idempotencyKey, WithdrawalRequest request) {
        WithdrawalRequest normalizedRequest = normalizeRequest(request);
        String normalizedKey = idempotencyKey.strip();
        String requestHash = hashRequest(normalizedRequest);

        return idempotencyKeyRepository.findById(normalizedKey)
                .flatMap(existing -> existing.getExpiresAt().isBefore(Instant.now())
                        ? idempotencyKeyRepository.deleteById(normalizedKey).then(Mono.empty())
                        : resolveIdempotency(existing, requestHash))
                .switchIfEmpty(Mono.defer(() -> createIdempotentWithdrawal(
                        normalizedKey, requestHash, normalizedRequest)))
                .as(transactionalOperator::transactional)
                .onErrorResume(DuplicateKeyException.class, error ->
                        idempotencyKeyRepository.findById(normalizedKey)
                                .flatMap(existing -> resolveIdempotency(existing, requestHash))
                                .switchIfEmpty(Mono.error(error)));
    }

    private WithdrawalRequest normalizeRequest(WithdrawalRequest request) {
        return new WithdrawalRequest(
                request.accountId(),
                request.amount().stripTrailingZeros(),
                request.currency().toUpperCase(Locale.ROOT));
    }

    private Mono<WithdrawalInitiationResult> resolveIdempotency(IdempotencyKey key, String requestHash) {
        if (!key.getRequestHash().equals(requestHash)) {
            return Mono.error(new IdempotencyConflictException("Idempotency key was already used for a different withdrawal request"));
        }

        if (key.getResponseBody() == null || key.getResponseStatus() == null) {
            return Mono.error(new IdempotencyConflictException("A withdrawal request with this idempotency key is still in progress"));
        }

        return Mono.fromCallable(() -> objectMapper.readValue(key.getResponseBody().asString(), Payment.class))
                .onErrorMap(JacksonException.class,
                        error -> new IllegalStateException("Failed to read stored idempotent withdrawal response", error))
                .map(payment -> new WithdrawalInitiationResult(payment, key.getResponseStatus()));
    }

    private Mono<WithdrawalInitiationResult> createIdempotentWithdrawal(
            String idempotencyKey,
            String requestHash,
            WithdrawalRequest request) {
        Instant now = Instant.now();
        return paymentRepository.save(buildWithdrawal(request, now))
                .flatMap(payment -> paymentEventOutboxService.saveWithdrawalRequested(payment)
                        .then(Mono.fromCallable(() -> Json.of(objectMapper.writeValueAsString(payment)))
                                .flatMap(responseBody -> idempotencyKeyRepository.create(
                                        idempotencyKey,
                                        requestHash,
                                        201,
                                        responseBody,
                                        payment.getId(),
                                        now,
                                        now.plus(24, ChronoUnit.HOURS)))
                                .flatMap(inserted -> inserted == 1
                                        ? Mono.just(new WithdrawalInitiationResult(payment, 201))
                                        : Mono.error(new IllegalStateException("Could not create withdrawal idempotency record")))))
                .onErrorMap(JacksonException.class,
                        error -> new IllegalStateException("Failed to serialize withdrawal response", error));
    }

    private Payment buildWithdrawal(WithdrawalRequest request, Instant createdAt) {
        return Payment.builder()
                .accountId(request.accountId())
                .type(PaymentType.WITHDRAWAL)
                .amount(request.amount())
                .currency(request.currency())
                .status(PaymentStatus.PENDING)
                .createdAt(createdAt)
                .build();
    }

    private String hashRequest(WithdrawalRequest request) {
        try {
            String normalizedBody = objectMapper.writeValueAsString(request);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(normalizedBody.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException | JacksonException error) {
            throw new IllegalStateException("Could not hash withdrawal request body", error);
        }
    }
}
