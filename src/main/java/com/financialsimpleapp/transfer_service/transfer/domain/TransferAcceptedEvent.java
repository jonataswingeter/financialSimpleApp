package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.Objects;
import java.util.UUID;

/**
 * Published once a transfer has been accepted; triggers its processing.
 */
public record TransferAcceptedEvent(
        UUID eventId,
        UUID transferId
) {

    public static final String TYPE = "TRANSFER_ACCEPTED";

    public TransferAcceptedEvent {
        Objects.requireNonNull(eventId, "Event ID is required");
        Objects.requireNonNull(transferId, "Transfer ID is required");
    }
}
