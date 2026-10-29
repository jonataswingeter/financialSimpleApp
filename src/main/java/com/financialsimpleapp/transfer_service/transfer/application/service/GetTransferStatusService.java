package com.financialsimpleapp.transfer_service.transfer.application.service;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.GetTransferStatusUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferNotFoundException;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferStatusResult;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferStatusData;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class GetTransferStatusService implements GetTransferStatusUseCase {

  private final TransferRepository transferRepository;

  public GetTransferStatusService(TransferRepository transferRepository) {
    this.transferRepository = transferRepository;
  }

  @Override
  public TransferStatusResult getTransferStatus(UUID transferId) {
    Objects.requireNonNull(transferId, "Transfer ID is required");

    TransferStatusData result =
        transferRepository
            .findStatusById(transferId)
            .orElseThrow(() -> new TransferNotFoundException(transferId));
    return new TransferStatusResult(
        result.transferId(),
        result.status(),
        result.rejectionCode(),
        result.createdAt(),
        result.completedAt());
  }
}
