package com.itplamen.digitalwallet.paymentservice.controller;

import com.itplamen.digitalwallet.paymentservice.dto.WithdrawalInitiationResult;
import com.itplamen.digitalwallet.paymentservice.dto.WithdrawalRequest;
import com.itplamen.digitalwallet.paymentservice.dto.WithdrawalResponse;
import com.itplamen.digitalwallet.paymentservice.service.PaymentService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@Validated
@RequestMapping("/api/v1/withdrawals")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping
    public Mono<ResponseEntity<WithdrawalResponse>> createWithdrawal(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 255) String idempotencyKey,
            @Valid @RequestBody WithdrawalRequest request) {
        return paymentService.createWithdrawal(idempotencyKey, request)
                .map(result -> ResponseEntity.status(result.responseStatus()).body(result.response()));
    }
}
