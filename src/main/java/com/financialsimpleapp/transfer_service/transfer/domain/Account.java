package com.financialsimpleapp.transfer_service.transfer.domain;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Objects;
import java.util.UUID;

public class Account {

  private final UUID id;
  private final Currency currency;
  private final AccountStatus status;
  private BigDecimal balance;

  public Account(UUID id, Currency currency, BigDecimal balance, AccountStatus status) {
    this.id = Objects.requireNonNull(id, "Account ID is required");
    this.currency = Objects.requireNonNull(currency, "Currency is required");
    this.balance = Objects.requireNonNull(balance, "Balance is required");
    this.status = Objects.requireNonNull(status, "Status is required");

    if (balance.signum() < 0) {
      throw new IllegalArgumentException("Balance cannot be negative");
    }
  }

  public boolean isActive() {
    return status == AccountStatus.ACTIVE;
  }

  public boolean hasFunds(BigDecimal amount) {
    return balance.compareTo(amount) >= 0;
  }

  public void debit(BigDecimal amount) {
    requirePositive(amount);
    requireActive();

    if (!hasFunds(amount)) {
      throw new IllegalStateException("Insufficient funds in account " + id);
    }

    balance = balance.subtract(amount);
  }

  public void credit(BigDecimal amount) {
    requirePositive(amount);
    requireActive();

    balance = balance.add(amount);
  }

  private void requireActive() {
    if (!isActive()) {
      throw new IllegalStateException("Account " + id + " is not active");
    }
  }

  private static void requirePositive(BigDecimal amount) {
    if (amount == null || amount.signum() <= 0) {
      throw new IllegalArgumentException("Amount must be positive");
    }
  }

  public UUID getId() {
    return id;
  }

  public Currency getCurrency() {
    return currency;
  }

  public BigDecimal getBalance() {
    return balance;
  }

  public AccountStatus getStatus() {
    return status;
  }
}
