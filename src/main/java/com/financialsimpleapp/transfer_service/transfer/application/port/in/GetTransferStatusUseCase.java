package com.financialsimpleapp.transfer_service.transfer.application.port.in;

import java.util.UUID;

public interface GetTransferStatusUseCase {

  TransferStatusResult getTransferStatus(UUID transferId);
}
