package com.itplamen.digitalwallet.bankmockservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class PaymentWebhookClientConfig {

    @Bean
    public WebClient paymentWebhookWebClient(@Value("${bank-mock.webhook.base-url}") String baseUrl) {
        return WebClient.builder().baseUrl(baseUrl).build();
    }
}
