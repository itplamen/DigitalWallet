package com.itplamen.digitalwallet.bankmockservice.controller;

import com.itplamen.digitalwallet.bankmockservice.dto.PayoutRequest;
import com.itplamen.digitalwallet.bankmockservice.dto.PayoutResponse;
import com.itplamen.digitalwallet.bankmockservice.service.PayoutService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class PayoutController {

    private final PayoutService payoutService;

    @PostMapping("/payouts")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PayoutResponse requestPayout(@RequestHeader("Idempotency-Key") String idempotencyKey,
                                        @Valid @RequestBody PayoutRequest request) {
        return payoutService.accept(idempotencyKey, request);
    }
}
