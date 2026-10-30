package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Aggregate root of a transfer request. Status flow: ACCEPTED -> (PROCESSING) -> COMPLETED |
 * REJECTED. PROCESSING only exists inside {@link #execute}; it is never persisted.
 *
 * <p>Package-private on purpose: outside the domain a transfer is only seen through one of its role
 * contracts, so each use case can only call its own part of the lifecycle:
 *
 * <ul>
 *   <li>{@link AcceptedTransfer}: created by {@code accept}, hands out the recorded event;
 *   <li>{@link ProcessableTransfer}: rebuilt by {@code restore}, finished by {@code execute} or
 *       {@code rejectForMissingAccount};
 *   <li>{@link TransferState}: the read-only state both share, used by persistence.
 * </ul>
 *
 * <p>The contracts (pre/postconditions) are documented on the interfaces and checked here.
 */
final class Transfer implements AcceptedTransfer, ProcessableTransfer {

  private final UUID id;
  private final UUID clientId;
  private final UUID fromAccountId;
  private final UUID toAccountId;
  private final Money amount;

  private TransferStatus status;
  private RejectionReason rejectionReason;

  private final List<TransferAcceptedEvent> events = new ArrayList<>();

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

  static Transfer accept(
      UUID id, UUID clientId, UUID fromAccountId, UUID toAccountId, Money amount) {
    Transfer transfer =
        new Transfer(
            id, clientId, fromAccountId, toAccountId, amount, TransferStatus.ACCEPTED, null);

    transfer.events.add(new TransferAcceptedEvent(UUID.randomUUID(), id));

    return transfer;
  }

  static Transfer restore(
      UUID id,
      UUID clientId,
      UUID fromAccountId,
      UUID toAccountId,
      Money amount,
      TransferStatus status,
      RejectionReason rejectionReason) {
    return new Transfer(id, clientId, fromAccountId, toAccountId, amount, status, rejectionReason);
  }

  @Override
  public List<TransferAcceptedEvent> pullEvents() {
    List<TransferAcceptedEvent> pulled = List.copyOf(events);
    events.clear();
    return pulled;
  }

  @Override
  public boolean isFinal() {
    return status == TransferStatus.COMPLETED || status == TransferStatus.REJECTED;
  }

  @Override
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

  @Override
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

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public UUID getClientId() {
    return clientId;
  }

  @Override
  public UUID getFromAccountId() {
    return fromAccountId;
  }

  @Override
  public UUID getToAccountId() {
    return toAccountId;
  }

  @Override
  public Money getAmount() {
    return amount;
  }

  @Override
  public TransferStatus getStatus() {
    return status;
  }

  @Override
  public RejectionReason getRejectionReason() {
    return rejectionReason;
  }
}
