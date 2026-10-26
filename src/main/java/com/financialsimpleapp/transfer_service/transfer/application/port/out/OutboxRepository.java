package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository {

  void insertTransferAccepted(
      UUID eventId,
      UUID transferId);

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