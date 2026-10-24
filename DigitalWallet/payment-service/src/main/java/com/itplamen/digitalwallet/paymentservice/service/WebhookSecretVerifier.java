package com.itplamen.digitalwallet.paymentservice.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@Component
public class WebhookSecretVerifier {

    private final byte[] expectedSecret;

    public WebhookSecretVerifier(@Value("${payment.webhook.secret}") String secret) {
        this.expectedSecret = secret.getBytes(StandardCharsets.UTF_8);
    }

    public void verify(String providedSecret) {
        boolean valid = providedSecret != null
                && MessageDigest.isEqual(expectedSecret, providedSecret.getBytes(StandardCharsets.UTF_8));
        if (!valid) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid webhook secret");
        }
    }
}
