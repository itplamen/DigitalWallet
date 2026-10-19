package com.itplamen.digitalwallet.transfer.service;

import com.itplamen.digitalwallet.transfer.dto.TransferInitiationResult;
import com.itplamen.digitalwallet.transfer.dto.TransferRequest;
import com.itplamen.digitalwallet.transfer.dto.TransferResponse;
import com.itplamen.digitalwallet.transfer.entity.IdempotencyKey;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.exception.IdempotencyConflictException;
import com.itplamen.digitalwallet.transfer.repository.IdempotencyKeyRepository;
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
import java.util.function.Function;

@Service
@RequiredArgsConstructor
public class TransferIdempotencyService {

    private final ObjectMapper objectMapper;
    private final TransactionalOperator transactionalOperator;
    private final IdempotencyKeyRepository idempotencyKeyRepository;

    public Mono<TransferInitiationResult> execute(
            String idempotencyKey,
            TransferRequest request,
            Function<TransferRequest, Mono<Transfer>> transferOperation) {
        TransferRequest normalizedRequest = normalizeRequest(request);
        String normalizedKey = idempotencyKey.strip();
        String requestHash = hashRequest(normalizedRequest);

        return idempotencyKeyRepository.findById(normalizedKey)
                .flatMap(existing -> existing.getExpiresAt().isBefore(Instant.now())
                        ? idempotencyKeyRepository.deleteById(normalizedKey).then(Mono.empty())
                        : resolveIdempotency(existing, requestHash))
                .switchIfEmpty(Mono.defer(() -> createIdempotentTransfer(normalizedKey, requestHash, normalizedRequest, transferOperation)))
                .as(transactionalOperator::transactional)
                .onErrorResume(DuplicateKeyException.class, error ->
                        idempotencyKeyRepository.findById(normalizedKey)
                                .flatMap(existing -> resolveIdempotency(existing, requestHash))
                                .switchIfEmpty(Mono.error(error)));
    }

    private TransferRequest normalizeRequest(TransferRequest request) {
        return new TransferRequest(
                request.sourceAccountId(),
                request.targetAccountId(),
                request.amount().stripTrailingZeros(),
                request.currency().toUpperCase(Locale.ROOT));
    }

    private Mono<TransferInitiationResult> resolveIdempotency(IdempotencyKey key, String requestHash) {
        if (!key.getRequestHash().equals(requestHash)) {
            return Mono.error(new IdempotencyConflictException("Idempotency key was already used for a different transfer request"));
        }

        if (key.getResponseBody() == null || key.getResponseStatus() == null) {
            return Mono.error(new IdempotencyConflictException("A transfer request with this idempotency key is still in progress"));
        }

        return Mono.fromCallable(() -> objectMapper.readValue(key.getResponseBody().asString(), TransferResponse.class))
                .onErrorMap(JacksonException.class,
                        error -> new IllegalStateException("Failed to read stored idempotent response", error))
                .map(response -> new TransferInitiationResult(response, key.getResponseStatus()));
    }

    private Mono<TransferInitiationResult> createIdempotentTransfer(
            String idempotencyKey,
            String requestHash,
            TransferRequest request,
            Function<TransferRequest, Mono<Transfer>> transferOperation) {
        Instant now = Instant.now();
        return transferOperation.apply(request)
                .flatMap(transfer -> {
                    TransferResponse response = new TransferResponse(transfer.getId(), transfer.getStatus());
                    return Mono.fromCallable(() -> Json.of(objectMapper.writeValueAsString(response)))
                        .flatMap(responseBody -> idempotencyKeyRepository.create(
                                        idempotencyKey,
                                        requestHash,
                                        201,
                                        responseBody,
                                        transfer.getId(),
                                        now,
                                        now.plus(24, ChronoUnit.HOURS))
                                .flatMap(inserted -> inserted == 1
                                        ? Mono.just(new TransferInitiationResult(response, 201))
                                        : Mono.error(new IllegalStateException("Could not create idempotency record"))));
                })
                .onErrorMap(JacksonException.class,
                        error -> new IllegalStateException("Failed to serialize transfer response", error));
    }

    private String hashRequest(TransferRequest request) {
        try {
            String normalizedBody = objectMapper.writeValueAsString(request);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(normalizedBody.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException | JacksonException error) {
            throw new IllegalStateException("Could not hash transfer request body", error);
        }
    }
}
