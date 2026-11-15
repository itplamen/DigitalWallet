package com.itplamen.digitalwallet.bankmockservice.controller;

import com.itplamen.digitalwallet.bankmockservice.dto.ChargeRequest;
import com.itplamen.digitalwallet.bankmockservice.dto.ChargeResponse;
import com.itplamen.digitalwallet.bankmockservice.service.ChargeService;
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
public class ChargeController {

    private final ChargeService chargeService;

    @PostMapping("/charges")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ChargeResponse requestCharge(@RequestHeader("Idempotency-Key") String idempotencyKey,
                                        @Valid @RequestBody ChargeRequest request) {
        return chargeService.accept(idempotencyKey, request);
    }
}
