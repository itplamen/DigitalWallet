package com.itplamen.digitalwallet.notificationservice.service;

import com.itplamen.digitalwallet.notificationservice.dto.NotificationEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class SseNotificationHub {

    private final ConcurrentMap<UUID, Sinks.Many<NotificationEvent>> accountSinks = new ConcurrentHashMap<>();

    public Flux<NotificationEvent> stream(UUID accountId) {
        return Flux.defer(() -> {
            Sinks.Many<NotificationEvent> sink = accountSinks.computeIfAbsent(accountId, ignored -> Sinks.many().multicast().directBestEffort());

            return sink.asFlux().doFinally(signal -> {
                if (sink.currentSubscriberCount() == 0) {
                    accountSinks.remove(accountId, sink);
                }
            });
        });
    }

    public boolean emit(UUID accountId, NotificationEvent event) {
        Sinks.Many<NotificationEvent> sink = accountSinks.get(accountId);
        return sink != null && sink.tryEmitNext(event).isSuccess();
    }
}
