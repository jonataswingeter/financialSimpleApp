package com.financialsimpleapp.transfer_service.transfer.infrastructure.messaging;

import java.util.UUID;

public record TransferAcceptedEvent(
    UUID eventId,
    UUID transferId
) {}
