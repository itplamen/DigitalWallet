package com.itplamen.digitalwallet.notificationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "notification.email", name = "smtp-enabled", havingValue = "false", matchIfMissing = true)
public class LoggingEmailSender implements EmailSender {

    @Override
    public Mono<Void> send(String to, String subject, String body) {
        return Mono.fromRunnable(() -> log.info("Mock email sender accepted outbound message: {}", subject));
    }
}
