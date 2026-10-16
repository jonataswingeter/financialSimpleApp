package com.financialsimpleapp.transfer_service.transfer.application.port;

import java.util.UUID;

public interface OutboxRepository {

  void insertTransferAccepted(
      UUID eventId,
      UUID transferId);

}