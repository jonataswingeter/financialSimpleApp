package com.financialsimpleapp.transfer_service.transfer.adapter.out.outbox;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Sends an outbox payload to Kafka and blocks until the broker confirms it. */
@Component
public class KafkaOutboxPublisher {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;
  private final Duration sendTimeout;

  public KafkaOutboxPublisher(
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${app.kafka.topics.transfer-accepted:transfer.accepted}") String topic,
      @Value("${app.outbox.send-timeout:PT10S}") Duration sendTimeout) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
    this.sendTimeout = sendTimeout;
  }

  /**
   * @throws EventPublishingException if the broker did not confirm in time
   */
  public void publish(UUID transferId, String payload) {
    try {
      // The transfer ID is the key, so all events of a transfer
      // land on the same partition and keep their order.
      kafkaTemplate
          .send(topic, transferId.toString(), payload)
          .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);

    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new EventPublishingException("Interrupted while publishing transfer " + transferId, ex);

    } catch (ExecutionException | TimeoutException ex) {
      throw new EventPublishingException("Kafka did not confirm transfer " + transferId, ex);
    }
  }
}
