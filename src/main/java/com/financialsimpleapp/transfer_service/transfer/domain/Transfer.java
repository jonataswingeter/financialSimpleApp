package com.financialsimpleapp.transfer_service.transfer.domain;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class Transfer {

  private final UUID id;
  private final UUID fromAccountId;
  private final UUID toAccountId;
  private final BigDecimal amount;
  private final Currency currency;

  private TransferStatus status;
  private RejectionReason rejectionReason;

  public Transfer(
      UUID id, UUID fromAccountId, UUID toAccountId, BigDecimal amount, Currency currency) {
    this(id, fromAccountId, toAccountId, amount, currency, TransferStatus.ACCEPTED, null);
  }

  private Transfer(
      UUID id,
      UUID fromAccountId,
      UUID toAccountId,
      BigDecimal amount,
      Currency currency,
      TransferStatus status,
      RejectionReason rejectionReason) {
    this.id = Objects.requireNonNull(id, "Transfer ID is required");
    this.fromAccountId = Objects.requireNonNull(fromAccountId, "Source account ID is required");
    this.toAccountId = Objects.requireNonNull(toAccountId, "Destination account ID is required");
    this.amount = Objects.requireNonNull(amount, "Amount is required");
    this.currency = Objects.requireNonNull(currency, "Currency is required");
    this.status = Objects.requireNonNull(status, "Status is required");

    if (fromAccountId.equals(toAccountId)) {
      throw new IllegalArgumentException("Source and destination accounts must be different");
    }

    if (amount.signum() <= 0) {
      throw new IllegalArgumentException("Transfer amount must be positive");
    }

    if (amount.scale() > 2) {
      throw new IllegalArgumentException("Transfer amount cannot have more than 2 decimal places");
    }

    if ((status == TransferStatus.REJECTED) != (rejectionReason != null)) {
      throw new IllegalArgumentException("Only rejected transfers have a rejection reason");
    }

    this.rejectionReason = rejectionReason;
  }

  /** Rebuilds a transfer that was already persisted. */
  public static Transfer restore(
      UUID id,
      UUID fromAccountId,
      UUID toAccountId,
      BigDecimal amount,
      Currency currency,
      TransferStatus status,
      RejectionReason rejectionReason) {
    return new Transfer(id, fromAccountId, toAccountId, amount, currency, status, rejectionReason);
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
        LedgerEntry.debit(id, source.getId(), amount, currency),
        LedgerEntry.credit(id, destination.getId(), amount, currency));
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

    if (!currency.equals(source.getCurrency()) || !currency.equals(destination.getCurrency())) {
      return RejectionReason.CURRENCY_MISMATCH;
    }

    if (!source.hasFunds(amount)) {
      return RejectionReason.INSUFFICIENT_FUNDS;
    }

    return null;
  }

  public void startProcessing() {
    if (status != TransferStatus.ACCEPTED) {
      throw new IllegalStateException("Only accepted transfers can start processing");
    }

    status = TransferStatus.PROCESSING;
  }

  public void complete() {
    if (status != TransferStatus.PROCESSING) {
      throw new IllegalStateException("Only processing transfers can be completed");
    }

    status = TransferStatus.COMPLETED;
    rejectionReason = null;
  }

  public void reject(RejectionReason reason) {
    if (status != TransferStatus.PROCESSING) {
      throw new IllegalStateException("Only processing transfers can be rejected");
    }

    status = TransferStatus.REJECTED;
    this.rejectionReason = Objects.requireNonNull(reason, "Rejection reason is required");
  }

  public UUID getId() {
    return id;
  }

  public UUID getFromAccountId() {
    return fromAccountId;
  }

  public UUID getToAccountId() {
    return toAccountId;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public Currency getCurrency() {
    return currency;
  }

  public TransferStatus getStatus() {
    return status;
  }

  public RejectionReason getRejectionReason() {
    return rejectionReason;
  }
}
