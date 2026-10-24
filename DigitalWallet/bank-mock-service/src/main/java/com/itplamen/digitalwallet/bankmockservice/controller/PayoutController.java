package com.itplamen.digitalwallet.bankmockservice.controller;

import com.itplamen.digitalwallet.bankmockservice.dto.PayoutRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PayoutController {

    @PostMapping("/payouts")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void requestPayout(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody PayoutRequest request) {
    }
}
