package com.itplamen.digitalwallet.paymentservice.controller;

import com.itplamen.digitalwallet.paymentservice.dto.DepositResponse;
import com.itplamen.digitalwallet.paymentservice.dto.PaymentRequest;
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
@RequestMapping("/api/v1/deposits")
@RequiredArgsConstructor
public class DepositController {

    private final PaymentService paymentService;

    @PostMapping
    public Mono<ResponseEntity<DepositResponse>> createDeposit(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 255) String idempotencyKey,
            @Valid @RequestBody PaymentRequest request) {
        return paymentService.createDeposit(idempotencyKey, request)
                .map(result -> ResponseEntity.status(result.responseStatus()).body(result.response()));
    }
}
