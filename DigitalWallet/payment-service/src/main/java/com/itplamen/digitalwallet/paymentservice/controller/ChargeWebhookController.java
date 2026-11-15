package com.itplamen.digitalwallet.paymentservice.controller;

import com.itplamen.digitalwallet.paymentservice.dto.ChargeFailedWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.dto.ChargeSucceededWebhookRequest;
import com.itplamen.digitalwallet.paymentservice.service.ChargeWebhookService;
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
@RequestMapping("/webhooks/charges")
@RequiredArgsConstructor
public class ChargeWebhookController {

    private static final String WEBHOOK_SECRET_HEADER = "X-Webhook-Secret";

    private final WebhookSecretVerifier webhookSecretVerifier;
    private final ChargeWebhookService chargeWebhookService;

    @PostMapping("/succeeded")
    public Mono<Void> chargeSucceeded(
            @RequestHeader(value = WEBHOOK_SECRET_HEADER, required = false) String secret,
            @Valid @RequestBody ChargeSucceededWebhookRequest request) {
        return Mono.fromRunnable(() -> webhookSecretVerifier.verify(secret))
                .then(Mono.defer(() -> chargeWebhookService.handleChargeSucceeded(request)));
    }

    @PostMapping("/failed")
    public Mono<Void> chargeFailed(
            @RequestHeader(value = WEBHOOK_SECRET_HEADER, required = false) String secret,
            @Valid @RequestBody ChargeFailedWebhookRequest request) {
        return Mono.fromRunnable(() -> webhookSecretVerifier.verify(secret))
                .then(Mono.defer(() -> chargeWebhookService.handleChargeFailed(request)));
    }
}
