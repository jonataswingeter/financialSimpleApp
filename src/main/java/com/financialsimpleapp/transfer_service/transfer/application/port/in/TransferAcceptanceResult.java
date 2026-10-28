package com.financialsimpleapp.transfer_service.transfer.application.port.in;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.util.UUID;

public record TransferAcceptanceResult(UUID transferId, TransferStatus status, boolean replayed) {}
