package com.financialsimpleapp.transfer_service.transfer.application.service;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.RelayOutboxEventsUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.ClaimedOutboxEvent;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferEventPublisher;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Relays outbox events to the broker.
 *
 * <p>Deliberately not {@code @Transactional}: the broker must never be called while a database
 * transaction is open. Claiming, marking and rescheduling each run in their own short transaction.
 */
@Service
public class OutboxRelayService implements RelayOutboxEventsUseCase {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);

  private final OutboxTransactionService outboxTransactions;
  private final TransferEventPublisher eventPublisher;
  private final OutboxRelaySettings settings;

  public OutboxRelayService(
      OutboxTransactionService outboxTransactions,
      TransferEventPublisher eventPublisher,
      OutboxRelaySettings settings) {
    this.outboxTransactions = outboxTransactions;
    this.eventPublisher = eventPublisher;
    this.settings = settings;
  }

  @Override
  public int relayPendingEvents() {
    final List<ClaimedOutboxEvent> events;

    try {
      events = outboxTransactions.claimPending(settings.batchSize(), settings.leaseDuration());
    } catch (RuntimeException ex) {
      log.error("Failed to claim outbox events", ex);
      return 0;
    }

    int published = 0;

    for (ClaimedOutboxEvent event : events) {
      if (Thread.currentThread().isInterrupted()) {
        // The remaining leases expire and the events are claimed again.
        break;
      }

      if (relay(event)) {
        published++;
      }
    }

    return published;
  }

  private boolean relay(ClaimedOutboxEvent event) {
    try {
      eventPublisher.publish(event.transferId(), event.payload());
    } catch (RuntimeException publishException) {
      scheduleRetryAfterPublishFailure(event, publishException);
      return false;
    }

    // O Kafka confirmou o envio. Agora atualizamos o PostgreSQL
    // em uma transação separada.
    try {
      boolean marked = outboxTransactions.markPublished(event.eventId(), event.lockToken());

      if (!marked) {
        log.warn("Broker accepted event {}, but its outbox lease was lost", event.eventId());
      }
    } catch (RuntimeException databaseException) {
      // Não agendamos uma retentativa imediata aqui:
      // o Kafka já confirmou o envio. O evento poderá ser
      // recuperado quando o lease expirar.
      log.error(
          "Broker accepted event {}, but marking it as published failed",
          event.eventId(),
          databaseException);
    }

    return true;
  }

  private void scheduleRetryAfterPublishFailure(
      ClaimedOutboxEvent event, RuntimeException exception) {

    Duration delay = retryDelay(event.attempts());

    try {
      boolean scheduled =
          outboxTransactions.scheduleRetry(event.eventId(), event.lockToken(), delay);

      if (scheduled) {
        log.warn(
            "Publishing event {} failed; retry scheduled in {} ms",
            event.eventId(),
            delay.toMillis(),
            exception);
      } else {
        log.warn(
            "Could not schedule retry for event {}; its lease may have expired",
            event.eventId(),
            exception);
      }
    } catch (RuntimeException retryException) {
      log.error("Failed to schedule retry for event {}", event.eventId(), retryException);
    }
  }

  /** Exponential backoff: retryBase * 2^attempts, capped at retryMax. */
  Duration retryDelay(int attempts) {
    long baseMillis = settings.retryBase().toMillis();
    long maxMillis = settings.retryMax().toMillis();

    int exponent = Math.max(0, Math.min(attempts, 20));
    long multiplier = 1L << exponent;

    if (baseMillis > maxMillis / multiplier) {
      return Duration.ofMillis(maxMillis);
    }

    return Duration.ofMillis(Math.min(baseMillis * multiplier, maxMillis));
  }
}
