package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import java.util.UUID;

public record ClaimedOutboxEvent(
    UUID eventId,
    UUID transferId,
    String eventType,
    String payload,
    int attempts,
    UUID lockToken) {}
