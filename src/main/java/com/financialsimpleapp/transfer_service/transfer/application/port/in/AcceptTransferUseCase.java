package com.financialsimpleapp.transfer_service.transfer.application.port.in;

public interface AcceptTransferUseCase {

  TransferAcceptanceResult acceptTransfer(AcceptTransferCommand command);
}
