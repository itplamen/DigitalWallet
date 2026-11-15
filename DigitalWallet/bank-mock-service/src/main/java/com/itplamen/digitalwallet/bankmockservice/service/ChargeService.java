package com.itplamen.digitalwallet.bankmockservice.service;

import com.itplamen.digitalwallet.bankmockservice.client.PaymentWebhookClient;
import com.itplamen.digitalwallet.bankmockservice.dto.ChargeRequest;
import com.itplamen.digitalwallet.bankmockservice.dto.ChargeResponse;
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
public class ChargeService {

    private final ConcurrentMap<String, UUID> chargeIdsByIdempotencyKey = new ConcurrentHashMap<>();
    private final PaymentWebhookClient paymentWebhookClient;
    private final Duration webhookDelay;

    public ChargeService(
            PaymentWebhookClient paymentWebhookClient,
            @Value("${bank-mock.webhook.delay}") Duration webhookDelay) {
        this.paymentWebhookClient = paymentWebhookClient;
        this.webhookDelay = webhookDelay;
    }

    public ChargeResponse accept(String idempotencyKey, ChargeRequest request) {
        UUID[] created = new UUID[1];
        UUID chargeId = chargeIdsByIdempotencyKey.computeIfAbsent(idempotencyKey, ignored -> created[0] = UUID.randomUUID());

        if (created[0] != null) {
            scheduleWebhook(chargeId, request);
        }

        return new ChargeResponse(chargeId);
    }

    private void scheduleWebhook(UUID chargeId, ChargeRequest request) {
        Mono.delay(webhookDelay)
                .then(paymentWebhookClient.notifyChargeSucceeded(chargeId, request.paymentId()))
                .doOnError(error -> log.error("Failed to notify charge {} result for payment {}", chargeId, request.paymentId(), error))
                .onErrorComplete()
                .subscribe();
    }
}
