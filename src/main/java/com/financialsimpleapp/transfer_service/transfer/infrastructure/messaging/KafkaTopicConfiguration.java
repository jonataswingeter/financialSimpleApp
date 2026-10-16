package com.financialsimpleapp.transfer_service.transfer.infrastructure.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfiguration {

  @Bean
  public NewTopic transfersAcceptedTopic() {
    return TopicBuilder.name("transfers.accepted.v1")
        .partitions(3)
        .replicas(3)
        .config("min.insync.replicas", "2")
        .build();
  }
}