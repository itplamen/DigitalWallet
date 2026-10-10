package com.itplamen.digitalwallet.transfer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic transferEventsTopic() {
        return TopicBuilder.name("transfer-events")
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "604800000")
                .build();
    }
}