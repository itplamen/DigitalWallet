package com.itplamen.digitalwallet.bankmockservice.service;

import com.itplamen.digitalwallet.bankmockservice.client.PaymentWebhookClient;
import com.itplamen.digitalwallet.bankmockservice.dto.PayoutRequest;
import com.itplamen.digitalwallet.bankmockservice.dto.PayoutResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Service
public class PayoutService {

    private static final String AMOUNT_LIMIT_EXCEEDED = "AMOUNT_LIMIT_EXCEEDED";

    private final ConcurrentMap<String, UUID> payoutIdsByIdempotencyKey = new ConcurrentHashMap<>();
    private final PaymentWebhookClient paymentWebhookClient;
    private final Duration webhookDelay;
    private final BigDecimal maxAmount;

    public PayoutService(
            PaymentWebhookClient paymentWebhookClient,
            @Value("${bank-mock.webhook.delay}") Duration webhookDelay,
            @Value("${bank-mock.payout.max-amount}") BigDecimal maxAmount) {
        this.paymentWebhookClient = paymentWebhookClient;
        this.webhookDelay = webhookDelay;
        this.maxAmount = maxAmount;
    }

    public PayoutResponse accept(String idempotencyKey, PayoutRequest request) {
        UUID[] created = new UUID[1];
        UUID payoutId = payoutIdsByIdempotencyKey.computeIfAbsent(idempotencyKey, ignored -> created[0] = UUID.randomUUID());

        if (created[0] != null) {
            scheduleWebhook(payoutId, request);
        }

        return new PayoutResponse(payoutId);
    }

    private void scheduleWebhook(UUID payoutId, PayoutRequest request) {
        Mono<Void> webhook = request.amount().compareTo(maxAmount) > 0
                ? paymentWebhookClient.notifyPayoutFailed(payoutId, request.paymentId(), AMOUNT_LIMIT_EXCEEDED)
                : paymentWebhookClient.notifyPayoutSucceeded(payoutId, request.paymentId());

        Mono.delay(webhookDelay)
                .then(webhook)
                .doOnError(error -> log.error("Failed to notify payout {} result for payment {}", payoutId, request.paymentId(), error))
                .onErrorComplete()
                .subscribe();
    }
}
