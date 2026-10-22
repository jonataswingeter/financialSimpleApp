package com.financialsimpleapp.transfer_service.transfer.application;

import com.financialsimpleapp.transfer_service.transfer.application.port.ClaimedOutboxEvent;
import com.financialsimpleapp.transfer_service.transfer.application.port.OutboxRepository;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxTransactionService {

  private final OutboxRepository outboxRepository;

  public OutboxTransactionService(OutboxRepository outboxRepository) {
    this.outboxRepository = outboxRepository;
  }

  @Transactional
  public List<ClaimedOutboxEvent> claimPending(
      int batchSize,
      Duration lockDuration) {
    return outboxRepository.claimPending(batchSize, lockDuration);
  }

  @Transactional
  public boolean markPublished(UUID eventId, UUID lockToken) {
    return outboxRepository.markPublished(eventId, lockToken);
  }

  @Transactional
  public boolean scheduleRetry(
      UUID eventId,
      UUID lockToken,
      Duration delay) {
    return outboxRepository.scheduleRetry(eventId, lockToken, delay);
  }
}
