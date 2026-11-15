package com.itplamen.digitalwallet.paymentservice.client;

import com.itplamen.digitalwallet.paymentservice.dto.ChargeRequest;
import com.itplamen.digitalwallet.paymentservice.entity.Payment;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class BankMockChargeClient {

    private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    private final WebClient bankMockWebClient;

    public Mono<Void> requestCharge(Payment payment) {
        return bankMockWebClient.post()
                .uri("/charges")
                .header(IDEMPOTENCY_KEY_HEADER, "charge-" + payment.getId())
                .bodyValue(new ChargeRequest(payment.getId(), payment.getAmount(), payment.getCurrency()))
                .retrieve()
                .toBodilessEntity()
                .then();
    }
}
