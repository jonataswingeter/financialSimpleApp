package com.financialsimpleapp.transfer_service.transfer.infrastructure.messaging;

import com.financialsimpleapp.transfer_service.transfer.application.OutboxTransactionService;
import com.financialsimpleapp.transfer_service.transfer.application.port.ClaimedOutboxEvent;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(name = "app.outbox.scheduling.enabled", havingValue = "true", matchIfMissing = true)
@Component
public class OutboxPublisher {

  private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

  private final OutboxTransactionService outboxTransactions;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;
  private final int batchSize;
  private final Duration lockDuration;
  private final Duration sendTimeout;
  private final long retryBaseMillis;
  private final long retryMaxMillis;

  public OutboxPublisher(
      OutboxTransactionService outboxTransactions,
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${app.kafka.topics.transfer-accepted:transfer.accepted}") String topic,
      @Value("${app.outbox.batch-size:100}") int batchSize,
      @Value("${app.outbox.lock-duration:PT30S}") Duration lockDuration,
      @Value("${app.outbox.send-timeout:PT10S}") Duration sendTimeout,
      @Value("${app.outbox.retry-base:PT1S}") Duration retryBase,
      @Value("${app.outbox.retry-max:PT5M}") Duration retryMax) {

    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    if (lockDuration.isZero() || lockDuration.isNegative()) {
      throw new IllegalArgumentException("lockDuration must be positive");
    }
    if (sendTimeout.isZero() || sendTimeout.isNegative()) {
      throw new IllegalArgumentException("sendTimeout must be positive");
    }
    if (retryBase.isZero() || retryBase.isNegative()
        || retryMax.compareTo(retryBase) < 0) {
      throw new IllegalArgumentException("Invalid retry durations");
    }

    this.outboxTransactions = outboxTransactions;
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
    this.batchSize = batchSize;
    this.lockDuration = calculateSafeLease(lockDuration, sendTimeout, batchSize);
    this.sendTimeout = sendTimeout;
    this.retryBaseMillis = retryBase.toMillis();
    this.retryMaxMillis = retryMax.toMillis();
  }

  @Scheduled(fixedDelayString = "${app.outbox.poll-delay-ms:1000}")
  public void publishBatch() {
    final List<ClaimedOutboxEvent> events;

    try {
      events = outboxTransactions.claimPending(batchSize, lockDuration);
    } catch (RuntimeException ex) {
      log.error("Failed to claim outbox events", ex);
      return;
    }

    for (ClaimedOutboxEvent event : events) {
      publishOne(event);
    }
  }

  private void publishOne(ClaimedOutboxEvent event) {
    try {
      kafkaTemplate.send(
          topic,
          event.transferId().toString(),
          event.payload())
          .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);

    } catch (Exception sendException) {
      scheduleRetryAfterSendFailure(event, sendException);
      return;
    }

    // O Kafka confirmou o envio. Agora atualizamos o PostgreSQL
    // em uma transação separada.
    try {
      boolean marked = outboxTransactions.markPublished(
          event.eventId(),
          event.lockToken());

      if (!marked) {
        log.warn(
            "Kafka accepted event {}, but its outbox lease was lost",
            event.eventId());
      }
    } catch (RuntimeException databaseException) {
      // Não agendamos uma retentativa imediata aqui:
      // o Kafka já confirmou o envio. O evento poderá ser
      // recuperado quando o lease expirar.
      log.error(
          "Kafka accepted event {}, but marking it as published failed",
          event.eventId(),
          databaseException);
    }
  }

  private void scheduleRetryAfterSendFailure(
      ClaimedOutboxEvent event,
      Exception exception) {

    long delayMillis = retryDelayMillis(event.attempts());

    try {
      boolean scheduled = outboxTransactions.scheduleRetry(
          event.eventId(),
          event.lockToken(),
          Duration.ofMillis(delayMillis));

      if (scheduled) {
        log.warn(
            "Publishing event {} failed; retry scheduled in {} ms",
            event.eventId(),
            delayMillis,
            exception);
      } else {
        log.warn(
            "Could not schedule retry for event {}; its lease may have expired",
            event.eventId(),
            exception);
      }
    } catch (RuntimeException retryException) {
      log.error(
          "Failed to schedule retry for event {}",
          event.eventId(),
          retryException);
    }

    if (exception instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private long retryDelayMillis(int attempts) {
    int exponent = Math.max(0, Math.min(attempts, 20));
    long multiplier = 1L << exponent;

    if (retryBaseMillis > retryMaxMillis / multiplier) {
      return retryMaxMillis;
    }

    return Math.min(retryBaseMillis * multiplier, retryMaxMillis);
  }

  private static Duration calculateSafeLease(
      Duration configuredLease,
      Duration sendTimeout,
      int batchSize) {

    Duration minimumLease = sendTimeout
        .multipliedBy(batchSize)
        .plusSeconds(30);

    return configuredLease.compareTo(minimumLease) >= 0
        ? configuredLease
        : minimumLease;
  }
}
