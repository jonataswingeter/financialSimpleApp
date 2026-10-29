package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.Objects;
import java.util.UUID;

/**
 * Recorded by {@link Transfer#accept} and published to Kafka; triggers the transfer's processing.
 *
 * <p>The record components are the wire format ({@code {"eventId","transferId"}}). Keep them
 * backward compatible: rows already in the outbox and messages already in Kafka use this shape.
 */
public record TransferAcceptedEvent(UUID eventId, UUID transferId) implements TransferEvent {

  public static final String TYPE = "TRANSFER_ACCEPTED";

  public TransferAcceptedEvent {
    Objects.requireNonNull(eventId, "Event ID is required");
    Objects.requireNonNull(transferId, "Transfer ID is required");
  }

  @Override
  public String type() {
    return TYPE;
  }
}
