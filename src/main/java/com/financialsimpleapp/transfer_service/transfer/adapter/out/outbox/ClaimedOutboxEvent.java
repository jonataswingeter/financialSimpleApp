package com.financialsimpleapp.transfer_service.transfer.adapter.out.outbox;

import java.util.UUID;

record ClaimedOutboxEvent(
    UUID eventId,
    UUID transferId,
    String eventType,
    String payload,
    int attempts,
    UUID lockToken) {}
