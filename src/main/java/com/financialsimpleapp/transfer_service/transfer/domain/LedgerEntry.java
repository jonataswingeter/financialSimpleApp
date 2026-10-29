package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.Objects;
import java.util.UUID;

/**
 * Immutable record of money leaving (DEBIT, negative amount) or entering (CREDIT, positive amount)
 * an account.
 */
public record LedgerEntry(UUID transferId, UUID accountId, LedgerEntryType type, Money amount) {

  public LedgerEntry {
    Objects.requireNonNull(transferId, "Transfer ID is required");
    Objects.requireNonNull(accountId, "Account ID is required");
    Objects.requireNonNull(type, "Entry type is required");
    Objects.requireNonNull(amount, "Amount is required");

    if (type == LedgerEntryType.DEBIT && !amount.isNegative()) {
      throw new IllegalArgumentException("Debit entries must have a negative amount");
    }

    if (type == LedgerEntryType.CREDIT && !amount.isPositive()) {
      throw new IllegalArgumentException("Credit entries must have a positive amount");
    }
  }

  public static LedgerEntry debit(UUID transferId, UUID accountId, Money amount) {
    return new LedgerEntry(transferId, accountId, LedgerEntryType.DEBIT, amount.negate());
  }

  public static LedgerEntry credit(UUID transferId, UUID accountId, Money amount) {
    return new LedgerEntry(transferId, accountId, LedgerEntryType.CREDIT, amount);
  }
}
