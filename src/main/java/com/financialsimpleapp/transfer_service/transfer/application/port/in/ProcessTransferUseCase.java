package com.financialsimpleapp.transfer_service.transfer.application.port.in;

import java.util.UUID;

public interface ProcessTransferUseCase {

  void process(UUID transferId);
}
