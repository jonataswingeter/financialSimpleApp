package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.List;
import java.util.UUID;

/**
 * The contract of a transfer seen by the process use case: a stored transfer that is finished by
 * moving the money or by rejecting it. It cannot be accepted again or record events.
 */
public interface ProcessableTransfer extends TransferState {

  /**
   * Rebuilds a transfer that was already persisted. Records no events.
   *
   * <p><b>Pre:</b> the same invariants as {@link AcceptedTransfer#accept}, and a rejection reason
   * if and only if the status is REJECTED.
   */
  static ProcessableTransfer restore(
      UUID id,
      UUID clientId,
      UUID fromAccountId,
      UUID toAccountId,
      Money amount,
      TransferStatus status,
      RejectionReason rejectionReason) {
    return Transfer.restore(
        id, clientId, fromAccountId, toAccountId, amount, status, rejectionReason);
  }

  /** COMPLETED or REJECTED; a final transfer is never processed again. */
  boolean isFinal();

  /**
   * Moves the money between the given accounts, or rejects the transfer when a business rule is
   * violated.
   *
   * <p><b>Pre:</b> status is ACCEPTED ({@link IllegalStateException} otherwise); {@code source} and
   * {@code destination} are this transfer's accounts ({@link IllegalArgumentException} otherwise);
   * the caller holds the locks on the transfer and both accounts. On a violated precondition
   * nothing changes.
   *
   * <p><b>Post:</b> status is COMPLETED, the source is debited and the destination credited by
   * {@link #getAmount()}, and one debit and one credit entry are returned; or status is REJECTED
   * with a reason, both accounts are unchanged and no entries are returned.
   *
   * @return the ledger entries to record; empty when rejected
   */
  List<LedgerEntry> execute(Account source, Account destination);

  /**
   * Rejects the transfer because one of its accounts does not exist.
   *
   * <p><b>Pre:</b> status is ACCEPTED ({@link IllegalStateException} otherwise). <b>Post:</b>
   * status REJECTED with {@link RejectionReason#ACCOUNT_NOT_FOUND}.
   */
  void rejectForMissingAccount();
}
