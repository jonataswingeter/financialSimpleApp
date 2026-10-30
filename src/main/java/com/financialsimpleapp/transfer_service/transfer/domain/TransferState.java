package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.UUID;

/**
 * Read-only view of a transfer, shared by its two roles ({@link AcceptedTransfer} and {@link
 * ProcessableTransfer}). This is all that persistence needs to store one.
 */
public interface TransferState {

  UUID getId();

  UUID getClientId();

  UUID getFromAccountId();

  UUID getToAccountId();

  Money getAmount();

  TransferStatus getStatus();

  /** Present if and only if the status is REJECTED. */
  RejectionReason getRejectionReason();
}
