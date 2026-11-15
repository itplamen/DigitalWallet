package com.itplamen.digitalwallet.bankmockservice.service;

import com.itplamen.digitalwallet.bankmockservice.dto.ChargeResponse;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class ChargeService {

    private final ConcurrentMap<String, UUID> chargeIdsByIdempotencyKey = new ConcurrentHashMap<>();

    public ChargeResponse accept(String idempotencyKey) {
        return new ChargeResponse(chargeIdsByIdempotencyKey.computeIfAbsent(idempotencyKey, ignored -> UUID.randomUUID()));
    }
}
