package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.List;
import java.util.UUID;

/**
 * The contract of a transfer seen by the accept use case: a new transfer and the event that will
 * trigger its processing. It cannot move money; that is {@link ProcessableTransfer}'s role.
 */
public interface AcceptedTransfer extends TransferState {

  /**
   * Accepts a new transfer request and records the event that will trigger its processing.
   *
   * <p><b>Pre:</b> all arguments present, distinct accounts, positive amount. <b>Post:</b> status
   * ACCEPTED, exactly one {@link TransferAcceptedEvent} to pull.
   */
  static AcceptedTransfer accept(
      UUID id, UUID clientId, UUID fromAccountId, UUID toAccountId, Money amount) {
    return Transfer.accept(id, clientId, fromAccountId, toAccountId, amount);
  }

  /** Returns the events recorded since the last call, and forgets them. */
  List<TransferAcceptedEvent> pullEvents();
}
