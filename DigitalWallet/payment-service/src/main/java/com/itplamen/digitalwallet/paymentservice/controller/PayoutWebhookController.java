package com.itplamen.digitalwallet.paymentservice.controller;

import com.itplamen.digitalwallet.paymentservice.dto.PayoutFailedWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.dto.PayoutSucceededWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.service.PayoutWebhookService;
import com.itplamen.digitalwallet.paymentservice.service.WebhookSecretVerifier;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/webhooks/payouts")
@RequiredArgsConstructor
public class PayoutWebhookController {

    private static final String WEBHOOK_SECRET_HEADER = "X-Webhook-Secret";

    private final WebhookSecretVerifier webhookSecretVerifier;
    private final PayoutWebhookService payoutWebhookService;

    @PostMapping("/succeeded")
    public Mono<Void> payoutSucceeded(
            @RequestHeader(value = WEBHOOK_SECRET_HEADER, required = false) String secret,
            @Valid @RequestBody PayoutSucceededWebhookRequest request) {
        return Mono.fromRunnable(() -> webhookSecretVerifier.verify(secret))
                .then(Mono.defer(() -> payoutWebhookService.handlePayoutSucceeded(request)));
    }

    @PostMapping("/failed")
    public Mono<Void> payoutFailed(
            @RequestHeader(value = WEBHOOK_SECRET_HEADER, required = false) String secret,
            @Valid @RequestBody PayoutFailedWebhookRequest request) {
        return Mono.fromRunnable(() -> webhookSecretVerifier.verify(secret))
                .then(Mono.defer(() -> payoutWebhookService.handlePayoutFailed(request)));
    }
}
