package com.itplamen.digitalwallet.notificationservice.repository;

import com.itplamen.digitalwallet.notificationservice.entity.NotificationRecord;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface NotificationRepository extends ReactiveCrudRepository<NotificationRecord, UUID> { }
