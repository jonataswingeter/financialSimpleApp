package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferAcceptedEvent;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository {

  void insertTransferAccepted(TransferAcceptedEvent event);

  List<ClaimedOutboxEvent> claimPending(
      int batchSize,
      Duration lockDuration);

  boolean markPublished(
      UUID eventId,
      UUID lockToken);

  boolean scheduleRetry(
      UUID eventId,
      UUID lockToken,
      Duration delay);
}