package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Aggregate root of a transfer request. Created by {@link #accept}, which records a {@link
 * TransferAcceptedEvent}; later loaded with {@link #restore} and finished by {@link #execute} or
 * {@link #rejectForMissingAccount}.
 *
 * <p>Status flow: ACCEPTED -> (PROCESSING) -> COMPLETED | REJECTED. PROCESSING only exists inside
 * {@link #execute}; it is never persisted.
 */
public class Transfer {

  private final UUID id;
  private final UUID clientId;
  private final UUID fromAccountId;
  private final UUID toAccountId;
  private final Money amount;

  private TransferStatus status;
  private RejectionReason rejectionReason;

  private final List<TransferEvent> events = new ArrayList<>();

  private Transfer(
      UUID id,
      UUID clientId,
      UUID fromAccountId,
      UUID toAccountId,
      Money amount,
      TransferStatus status,
      RejectionReason rejectionReason) {
    this.id = Objects.requireNonNull(id, "Transfer ID is required");
    this.clientId = Objects.requireNonNull(clientId, "Client ID is required");
    this.fromAccountId = Objects.requireNonNull(fromAccountId, "Source account ID is required");
    this.toAccountId = Objects.requireNonNull(toAccountId, "Destination account ID is required");
    this.amount = Objects.requireNonNull(amount, "Amount is required");
    this.status = Objects.requireNonNull(status, "Status is required");

    if (fromAccountId.equals(toAccountId)) {
      throw new IllegalArgumentException("Source and destination accounts must be different");
    }

    if (!amount.isPositive()) {
      throw new IllegalArgumentException("Transfer amount must be positive");
    }

    if ((status == TransferStatus.REJECTED) != (rejectionReason != null)) {
      throw new IllegalArgumentException("Only rejected transfers have a rejection reason");
    }

    this.rejectionReason = rejectionReason;
  }

  /** Accepts a new transfer request and records the event that will trigger its processing. */
  public static Transfer accept(
      UUID id, UUID clientId, UUID fromAccountId, UUID toAccountId, Money amount) {
    Transfer transfer =
        new Transfer(
            id, clientId, fromAccountId, toAccountId, amount, TransferStatus.ACCEPTED, null);

    transfer.events.add(new TransferAcceptedEvent(UUID.randomUUID(), id));

    return transfer;
  }

  /** Rebuilds a transfer that was already persisted. Records no events. */
  public static Transfer restore(
      UUID id,
      UUID clientId,
      UUID fromAccountId,
      UUID toAccountId,
      Money amount,
      TransferStatus status,
      RejectionReason rejectionReason) {
    return new Transfer(id, clientId, fromAccountId, toAccountId, amount, status, rejectionReason);
  }

  /** Returns the events recorded since the last call, and forgets them. */
  public List<TransferEvent> pullEvents() {
    List<TransferEvent> pulled = List.copyOf(events);
    events.clear();
    return pulled;
  }

  public boolean isFinal() {
    return status == TransferStatus.COMPLETED || status == TransferStatus.REJECTED;
  }

  /**
   * Moves the money between the given accounts, or rejects the transfer when a business rule is
   * violated.
   *
   * @return the ledger entries to record; empty when rejected
   */
  public List<LedgerEntry> execute(Account source, Account destination) {
    Objects.requireNonNull(source, "Source account is required");
    Objects.requireNonNull(destination, "Destination account is required");

    if (!source.getId().equals(fromAccountId) || !destination.getId().equals(toAccountId)) {
      throw new IllegalArgumentException("Accounts do not belong to transfer " + id);
    }

    startProcessing();

    RejectionReason reason = findRejectionReason(source, destination);

    if (reason != null) {
      reject(reason);
      return List.of();
    }

    source.debit(amount);
    destination.credit(amount);
    complete();

    return List.of(
        LedgerEntry.debit(id, source.getId(), amount),
        LedgerEntry.credit(id, destination.getId(), amount));
  }

  /** Rejects the transfer because one of its accounts does not exist. */
  public void rejectForMissingAccount() {
    startProcessing();
    reject(RejectionReason.ACCOUNT_NOT_FOUND);
  }

  private RejectionReason findRejectionReason(Account source, Account destination) {
    if (!source.isActive()) {
      return RejectionReason.SOURCE_ACCOUNT_NOT_ACTIVE;
    }

    if (!destination.isActive()) {
      return RejectionReason.DESTINATION_ACCOUNT_NOT_ACTIVE;
    }

    if (!amount.hasCurrency(source.getCurrency())
        || !amount.hasCurrency(destination.getCurrency())) {
      return RejectionReason.CURRENCY_MISMATCH;
    }

    if (!source.hasFunds(amount)) {
      return RejectionReason.INSUFFICIENT_FUNDS;
    }

    return null;
  }

  private void startProcessing() {
    if (status != TransferStatus.ACCEPTED) {
      throw new IllegalStateException("Only accepted transfers can start processing");
    }

    status = TransferStatus.PROCESSING;
  }

  private void complete() {
    status = TransferStatus.COMPLETED;
    rejectionReason = null;
  }

  private void reject(RejectionReason reason) {
    status = TransferStatus.REJECTED;
    rejectionReason = Objects.requireNonNull(reason, "Rejection reason is required");
  }

  public UUID getId() {
    return id;
  }

  public UUID getClientId() {
    return clientId;
  }

  public UUID getFromAccountId() {
    return fromAccountId;
  }

  public UUID getToAccountId() {
    return toAccountId;
  }

  public Money getAmount() {
    return amount;
  }

  public TransferStatus getStatus() {
    return status;
  }

  public RejectionReason getRejectionReason() {
    return rejectionReason;
  }
}
