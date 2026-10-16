package com.financialsimpleapp.transfer_service.transfer.application;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;

import java.time.Instant;
import java.util.UUID;

public record TransferStatusResult(
        UUID transferId,
        TransferStatus status,
        String rejectionCode,
        Instant createdAt,
        Instant completedAt
) {
}