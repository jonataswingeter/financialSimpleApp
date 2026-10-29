package com.financialsimpleapp.transfer_service.transfer.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Currency;
import java.util.Objects;

/**
 * An amount in a currency, always with 2 decimal places (the scale of every money column). The sign
 * is free: balances are never negative, but debit ledger entries are.
 *
 * <p>Arithmetic and comparisons refuse to mix currencies, so a wrong-currency debit cannot slip
 * through even if a caller forgets to check.
 */
public record Money(BigDecimal amount, Currency currency) {

  private static final int SCALE = 2;

  public Money {
    if (amount == null) {
      throw new IllegalArgumentException("Amount is required");
    }
    Objects.requireNonNull(currency, "Currency is required");

    try {
      amount = amount.setScale(SCALE, RoundingMode.UNNECESSARY);
    } catch (ArithmeticException exception) {
      throw new IllegalArgumentException(
          "Amount cannot have more than " + SCALE + " decimal places", exception);
    }
  }

  public static Money of(String amount, String currencyCode) {
    return new Money(new BigDecimal(amount), Currency.getInstance(currencyCode));
  }

  public static Money zero(Currency currency) {
    return new Money(BigDecimal.ZERO, currency);
  }

  public boolean isPositive() {
    return amount.signum() > 0;
  }

  public boolean isNegative() {
    return amount.signum() < 0;
  }

  public boolean isAtLeast(Money other) {
    requireSameCurrency(other);
    return amount.compareTo(other.amount) >= 0;
  }

  public boolean hasCurrency(Currency other) {
    return currency.equals(other);
  }

  public Money plus(Money other) {
    requireSameCurrency(other);
    return new Money(amount.add(other.amount), currency);
  }

  public Money minus(Money other) {
    requireSameCurrency(other);
    return new Money(amount.subtract(other.amount), currency);
  }

  public Money negate() {
    return new Money(amount.negate(), currency);
  }

  private void requireSameCurrency(Money other) {
    Objects.requireNonNull(other, "Money is required");
    if (!currency.equals(other.currency)) {
      throw new IllegalArgumentException(
          "Currency mismatch: " + currency + " and " + other.currency);
    }
  }

  @Override
  public String toString() {
    return amount.toPlainString() + " " + currency.getCurrencyCode();
  }
}
