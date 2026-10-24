package com.itplamen.digitalwallet.bankmockservice.service;

import com.itplamen.digitalwallet.bankmockservice.client.PaymentWebhookClient;
import com.itplamen.digitalwallet.bankmockservice.dto.PayoutRequest;
import com.itplamen.digitalwallet.bankmockservice.dto.PayoutResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Service
public class PayoutService {

    private final ConcurrentMap<String, UUID> payoutIdsByIdempotencyKey = new ConcurrentHashMap<>();
    private final PaymentWebhookClient paymentWebhookClient;
    private final Duration webhookDelay;

    public PayoutService(PaymentWebhookClient paymentWebhookClient, @Value("${bank-mock.webhook.delay}") Duration webhookDelay) {
        this.paymentWebhookClient = paymentWebhookClient;
        this.webhookDelay = webhookDelay;
    }

    public PayoutResponse accept(String idempotencyKey, PayoutRequest request) {
        UUID[] created = new UUID[1];
        UUID payoutId = payoutIdsByIdempotencyKey.computeIfAbsent(idempotencyKey, ignored -> created[0] = UUID.randomUUID());

        if (created[0] != null) {
            scheduleSuccessWebhook(payoutId, request.paymentId());
        }

        return new PayoutResponse(payoutId);
    }

    private void scheduleSuccessWebhook(UUID payoutId, UUID paymentId) {
        Mono.delay(webhookDelay)
                .then(Mono.defer(() -> paymentWebhookClient.notifyPayoutSucceeded(payoutId, paymentId)))
                .doOnError(error -> log.error("Failed to notify payout {} success for payment {}", payoutId, paymentId, error))
                .onErrorComplete()
                .subscribe();
    }
}
