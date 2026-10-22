package com.financialsimpleapp.transfer_service.transfer.infrastructure.messaging;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

  @Value("${app.kafka.replication-factor:3}")
  private int replicationFactor;

  @Value("${app.kafka.partitions:3}")
  private int partitions;

  @Bean
  NewTopic transferAcceptedTopic(
      @Value("${app.kafka.topics.transfer-accepted:transfer.accepted}") String topic) {
    return topic(topic);
  }

  @Bean
  NewTopic transferAcceptedDltTopic(
      @Value("${app.kafka.topics.transfer-accepted:transfer.accepted}") String topic) {
    return topic(topic + ".DLT");
  }

  private NewTopic topic(String name) {
    return TopicBuilder.name(name)
        .partitions(partitions)
        .replicas(replicationFactor)
        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
        .build();
  }

  @Bean
  DefaultErrorHandler kafkaErrorHandler(
      KafkaTemplate<String, String> kafkaTemplate) {

    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, exception) -> new TopicPartition(
            record.topic() + ".DLT",
            record.partition()));

    return new DefaultErrorHandler(
        recoverer,
        new FixedBackOff(1000L, 3L));
  }
}
