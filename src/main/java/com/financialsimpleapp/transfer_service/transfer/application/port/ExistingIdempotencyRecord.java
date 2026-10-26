package com.financialsimpleapp.transfer_service.transfer.application.port;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.util.UUID;

public record ExistingIdempotencyRecord(
    String requestHash,
    UUID transferId,
    TransferStatus status) {
}