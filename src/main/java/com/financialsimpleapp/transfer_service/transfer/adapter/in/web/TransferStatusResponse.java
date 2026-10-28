package com.financialsimpleapp.transfer_service.transfer.adapter.in.web;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.time.Instant;
import java.util.UUID;

public record TransferStatusResponse(
    UUID transferId,
    TransferStatus status,
    String rejectionCode,
    Instant createdAt,
    Instant completedAt) {}
