package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.UUID;

/**
 * Something that happened to a transfer, recorded by the {@link Transfer} aggregate and stored in
 * the outbox in the same transaction as the transfer itself.
 */
public sealed interface TransferEvent permits TransferAcceptedEvent {

  UUID eventId();

  UUID transferId();

  /** Stable name stored in {@code outbox_events.event_type}. */
  String type();
}
