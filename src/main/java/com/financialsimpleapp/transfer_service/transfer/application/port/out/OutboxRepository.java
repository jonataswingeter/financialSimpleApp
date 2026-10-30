package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferAcceptedEvent;

/**
 * The sending side of the transactional outbox. Delivering the stored events to the broker is
 * infrastructure (the outbox relay adapter) and not the application's concern.
 */
public interface OutboxRepository {

  /** Stores an event recorded by an aggregate, in the caller's transaction. */
  void append(TransferAcceptedEvent event);
}
