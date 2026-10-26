package com.financialsimpleapp.transfer_service.transfer.application.port.in;

public interface RelayOutboxEventsUseCase {

  /**
   * Publishes the outbox events that are due.
   *
   * @return how many events the broker confirmed
   */
  int relayPendingEvents();
}
