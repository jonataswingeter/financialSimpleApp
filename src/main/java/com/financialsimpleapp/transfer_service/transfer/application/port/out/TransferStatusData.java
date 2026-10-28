package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.time.Instant;
import java.util.UUID;

public record TransferStatusData(
    UUID transferId,
    TransferStatus status,
    String rejectionCode,
    Instant createdAt,
    Instant completedAt) {}
