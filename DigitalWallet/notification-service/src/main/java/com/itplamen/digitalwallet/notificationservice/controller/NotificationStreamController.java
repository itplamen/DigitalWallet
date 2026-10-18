package com.itplamen.digitalwallet.notificationservice.controller;

import com.itplamen.digitalwallet.notificationservice.dto.NotificationEvent;
import com.itplamen.digitalwallet.notificationservice.service.SseNotificationHub;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/notifications")
@RequiredArgsConstructor
public class NotificationStreamController {

    private final SseNotificationHub sseNotificationHub;

    @GetMapping(path = "/{accountId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<NotificationEvent>> stream(@PathVariable UUID accountId) {
        return sseNotificationHub.stream(accountId)
                .map(event -> ServerSentEvent.builder(event)
                        .id(event.eventId().toString())
                        .event(event.status())
                        .build());
    }
}
