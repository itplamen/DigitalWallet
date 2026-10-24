package com.itplamen.digitalwallet.bankmockservice.client;

import com.itplamen.digitalwallet.bankmockservice.dto.PayoutSucceededWebhook;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
public class PaymentWebhookClient {

    private static final String WEBHOOK_SECRET_HEADER = "X-Webhook-Secret";

    private final WebClient paymentWebhookWebClient;
    private final String secret;

    public PaymentWebhookClient(WebClient paymentWebhookWebClient, @Value("${bank-mock.webhook.secret}") String secret) {
        this.paymentWebhookWebClient = paymentWebhookWebClient;
        this.secret = secret;
    }

    public Mono<Void> notifyPayoutSucceeded(UUID payoutId, UUID paymentId) {
        return paymentWebhookWebClient.post()
                .uri("/webhooks/payouts/succeeded")
                .header(WEBHOOK_SECRET_HEADER, secret)
                .bodyValue(new PayoutSucceededWebhook(payoutId, paymentId))
                .retrieve()
                .toBodilessEntity()
                .then();
    }
}
