package com.financialsimpleapp.transfer_service.transfer.application.port;

import java.util.UUID;

public record ClaimedOutboxEvent(
    UUID eventId,
    UUID transferId,
    String eventType,
    String payload,
    int attempts,
    UUID lockToken) {
}