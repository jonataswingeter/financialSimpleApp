package com.financialsimpleapp.transfer_service.transfer.application.service;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.ClaimedOutboxEvent;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.OutboxRepository;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transaction boundary of the outbox relay: claiming, marking and rescheduling each commit on
 * their own, so the relay never calls the broker while a transaction is open. Not a use case.
 */
@Service
class OutboxTransactions {

  private final OutboxRepository outboxRepository;

  OutboxTransactions(OutboxRepository outboxRepository) {
    this.outboxRepository = outboxRepository;
  }

  @Transactional
  public List<ClaimedOutboxEvent> claimPending(int batchSize, Duration lockDuration) {
    return outboxRepository.claimPending(batchSize, lockDuration);
  }

  @Transactional
  public boolean markPublished(UUID eventId, UUID lockToken) {
    return outboxRepository.markPublished(eventId, lockToken);
  }

  @Transactional
  public boolean scheduleRetry(UUID eventId, UUID lockToken, Duration delay) {
    return outboxRepository.scheduleRetry(eventId, lockToken, delay);
  }
}
