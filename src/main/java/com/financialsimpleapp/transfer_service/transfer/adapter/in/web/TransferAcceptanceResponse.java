package com.financialsimpleapp.transfer_service.transfer.adapter.in.web;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.util.UUID;

public record TransferAcceptanceResponse(
    UUID transferId, TransferStatus status, boolean replayed) {}
