package com.itplamen.digitalwallet.paymentservice.service;

import com.itplamen.digitalwallet.paymentservice.dto.PaymentInitiationResult;
import com.itplamen.digitalwallet.paymentservice.dto.PaymentRequest;
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
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;

@Component
@RequiredArgsConstructor
public class IdempotentPaymentInitiator {

    private static final Duration KEY_TTL = Duration.ofHours(24);

    private final ObjectMapper objectMapper;
    private final TransactionalOperator transactionalOperator;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final PaymentRepository paymentRepository;

    public <R> Mono<PaymentInitiationResult<R>> initiate(String idempotencyKey, PaymentInitiation<R> initiation) {
        PaymentRequest request = normalize(initiation.request());
        String normalizedKey = idempotencyKey.strip();
        String requestHash = hash(initiation.type(), request);

        return idempotencyKeyRepository.findById(normalizedKey)
                .flatMap(existing -> existing.getExpiresAt().isBefore(Instant.now())
                        ? idempotencyKeyRepository.deleteById(normalizedKey).then(Mono.<PaymentInitiationResult<R>>empty())
                        : resolveIdempotency(existing, requestHash, initiation))
                .switchIfEmpty(Mono.defer(() -> create(normalizedKey, requestHash, request, initiation)))
                .as(transactionalOperator::transactional)
                .onErrorResume(DuplicateKeyException.class, error ->
                        idempotencyKeyRepository.findById(normalizedKey)
                                .flatMap(existing -> resolveIdempotency(existing, requestHash, initiation))
                                .switchIfEmpty(Mono.error(error)));
    }

    private PaymentRequest normalize(PaymentRequest request) {
        return new PaymentRequest(
                request.accountId(),
                request.amount().stripTrailingZeros(),
                request.currency().toUpperCase(Locale.ROOT));
    }

    private <R> Mono<PaymentInitiationResult<R>> resolveIdempotency(
            IdempotencyKey key, String requestHash, PaymentInitiation<R> initiation) {
        String label = initiation.type().name().toLowerCase(Locale.ROOT);
        if (!key.getRequestHash().equals(requestHash)) {
            return Mono.error(new IdempotencyConflictException("Idempotency key was already used for a different " + label + " request"));
        }

        if (key.getResponseBody() == null || key.getResponseStatus() == null) {
            return Mono.error(new IdempotencyConflictException("A " + label + " request with this idempotency key is still in progress"));
        }

        return Mono.fromCallable(() -> objectMapper.readValue(key.getResponseBody().asString(), initiation.responseType()))
                .onErrorMap(JacksonException.class,
                        error -> new IllegalStateException("Failed to read stored idempotent " + label + " response", error))
                .map(response -> new PaymentInitiationResult<>(response, key.getResponseStatus()));
    }

    private <R> Mono<PaymentInitiationResult<R>> create(
            String idempotencyKey, String requestHash, PaymentRequest request, PaymentInitiation<R> initiation) {
        Instant now = Instant.now();
        return paymentRepository.save(buildPayment(initiation.type(), request, now))
                .flatMap(payment -> {
                    R response = initiation.responseFactory().apply(payment);
                    return initiation.outboxWriter().apply(payment)
                            .then(Mono.fromCallable(() -> Json.of(objectMapper.writeValueAsString(response))))
                            .flatMap(responseBody -> idempotencyKeyRepository.create(
                                    idempotencyKey,
                                    requestHash,
                                    initiation.responseStatus(),
                                    responseBody,
                                    payment.getId(),
                                    now,
                                    now.plus(KEY_TTL)))
                            .flatMap(inserted -> inserted == 1
                                    ? Mono.just(new PaymentInitiationResult<>(response, initiation.responseStatus()))
                                    : Mono.<PaymentInitiationResult<R>>error(new IllegalStateException(
                                            "Could not create idempotency record for " + initiation.type())));
                })
                .onErrorMap(JacksonException.class,
                        error -> new IllegalStateException("Failed to serialize " + initiation.type() + " response", error));
    }

    private Payment buildPayment(PaymentType type, PaymentRequest request, Instant createdAt) {
        return Payment.builder()
                .accountId(request.accountId())
                .type(type)
                .amount(request.amount())
                .currency(request.currency())
                .status(PaymentStatus.PENDING)
                .createdAt(createdAt)
                .build();
    }

    private String hash(PaymentType type, PaymentRequest request) {
        try {
            String normalizedBody = type + ":" + objectMapper.writeValueAsString(request);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(normalizedBody.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException | JacksonException error) {
            throw new IllegalStateException("Could not hash " + type + " request body", error);
        }
    }
}
