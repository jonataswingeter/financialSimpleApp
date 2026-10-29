package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferEvent;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository {

  /** Stores an event recorded by an aggregate, in the caller's transaction. */
  void append(TransferEvent event);

  List<ClaimedOutboxEvent> claimPending(int batchSize, Duration lockDuration);

  boolean markPublished(UUID eventId, UUID lockToken);

  boolean scheduleRetry(UUID eventId, UUID lockToken, Duration delay);
}
