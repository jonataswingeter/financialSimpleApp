package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import java.util.UUID;

public interface TransferEventPublisher {

  /**
   * Sends the event and blocks until the broker confirms it.
   *
   * @throws EventPublishingException if the broker did not confirm in time
   */
  void publish(UUID transferId, String payload);
}
