package com.financialsimpleapp.transfer_service.transfer.application.port;

import java.time.Instant;
import java.util.UUID;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;

public record TransferStatusData(
    UUID transferId,
    TransferStatus status,
    String rejectionCode,
    Instant createdAt,
    Instant completedAt) {
}
