package com.financialsimpleapp.transfer_service.transfer.domain;

import java.util.Currency;
import java.util.Objects;
import java.util.UUID;

/**
 * This bounded context's view of an account: just what a transfer needs (currency, balance,
 * status). The account's currency is the currency of its balance.
 */
public class Account {

  private final UUID id;
  private final AccountStatus status;

  private Money balance;

  public Account(UUID id, Money balance, AccountStatus status) {
    this.id = Objects.requireNonNull(id, "Account ID is required");
    this.balance = Objects.requireNonNull(balance, "Balance is required");
    this.status = Objects.requireNonNull(status, "Status is required");

    if (balance.isNegative()) {
      throw new IllegalArgumentException("Balance cannot be negative");
    }
  }

  public boolean isActive() {
    return status == AccountStatus.ACTIVE;
  }

  public boolean hasFunds(Money amount) {
    return balance.isAtLeast(amount);
  }

  public void debit(Money amount) {
    requirePositive(amount);
    requireActive();

    if (!hasFunds(amount)) {
      throw new IllegalStateException("Insufficient funds in account " + id);
    }

    balance = balance.minus(amount);
  }

  public void credit(Money amount) {
    requirePositive(amount);
    requireActive();

    balance = balance.plus(amount);
  }

  private void requireActive() {
    if (!isActive()) {
      throw new IllegalStateException("Account " + id + " is not active");
    }
  }

  private static void requirePositive(Money amount) {
    if (amount == null || !amount.isPositive()) {
      throw new IllegalArgumentException("Amount must be positive");
    }
  }

  public UUID getId() {
    return id;
  }

  public Currency getCurrency() {
    return balance.currency();
  }

  public Money getBalance() {
    return balance;
  }

  public AccountStatus getStatus() {
    return status;
  }
}
