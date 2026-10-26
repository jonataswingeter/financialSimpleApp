package com.financialsimpleapp.transfer_service.transfer.infrastructure.messaging;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.EventPublishingException;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferEventPublisher;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransferEventPublisher implements TransferEventPublisher {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;
  private final Duration sendTimeout;

  public KafkaTransferEventPublisher(
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${app.kafka.topics.transfer-accepted:transfer.accepted}") String topic,
      @Value("${app.outbox.send-timeout:PT10S}") Duration sendTimeout) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
    this.sendTimeout = sendTimeout;
  }

  @Override
  public void publish(UUID transferId, String payload) {
    try {
      // The transfer ID is the key, so all events of a transfer
      // land on the same partition and keep their order.
      kafkaTemplate.send(topic, transferId.toString(), payload)
          .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);

    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new EventPublishingException(
          "Interrupted while publishing transfer " + transferId, ex);

    } catch (ExecutionException | TimeoutException ex) {
      throw new EventPublishingException(
          "Kafka did not confirm transfer " + transferId, ex);
    }
  }
}
