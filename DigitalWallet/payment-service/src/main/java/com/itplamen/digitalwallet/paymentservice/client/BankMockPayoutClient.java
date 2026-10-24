package com.itplamen.digitalwallet.paymentservice.client;

import com.itplamen.digitalwallet.paymentservice.dto.PayoutRequest;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class BankMockPayoutClient {

    private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    private final WebClient bankMockWebClient;

    public Mono<Void> requestPayout(Payment payment) {
        return bankMockWebClient.post()
                .uri("/payouts")
                .header(IDEMPOTENCY_KEY_HEADER, "payout-" + payment.getId())
                .bodyValue(new PayoutRequest(payment.getId(), payment.getAmount(), payment.getCurrency()))
                .retrieve()
                .toBodilessEntity()
                .then();
    }
}
