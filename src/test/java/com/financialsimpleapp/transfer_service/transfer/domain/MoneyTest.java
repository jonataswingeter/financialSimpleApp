package com.financialsimpleapp.transfer_service.transfer.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MoneyTest {

  private static final Currency EUR = Currency.getInstance("EUR");

  @Test
  void shouldNormalizeToTwoDecimalPlaces() {
    Money money = new Money(new BigDecimal("25"), EUR);

    assertEquals(new BigDecimal("25.00"), money.amount());
    assertEquals(Money.of("25.0", "EUR"), money, "Equal amounts are equal whatever their scale");
  }

  @Test
  void shouldRefuseMoreThanTwoDecimalPlaces() {
    assertThrows(IllegalArgumentException.class, () -> Money.of("25.001", "EUR"));
  }

  @Test
  void shouldRefuseMissingAmount() {
    assertThrows(IllegalArgumentException.class, () -> new Money(null, EUR));
  }

  @Test
  void shouldAddSubtractAndNegate() {
    Money ten = Money.of("10.00", "EUR");

    assertEquals(Money.of("12.50", "EUR"), ten.plus(Money.of("2.50", "EUR")));
    assertEquals(Money.of("7.50", "EUR"), ten.minus(Money.of("2.50", "EUR")));
    assertEquals(Money.of("-10.00", "EUR"), ten.negate());
  }

  @Test
  void shouldCompareAmounts() {
    Money ten = Money.of("10.00", "EUR");

    assertTrue(ten.isAtLeast(Money.of("10.00", "EUR")));
    assertFalse(ten.isAtLeast(Money.of("10.01", "EUR")));
    assertTrue(ten.isPositive());
    assertTrue(ten.negate().isNegative());
    assertFalse(Money.zero(EUR).isPositive());
    assertFalse(Money.zero(EUR).isNegative());
  }

  @Test
  void shouldRefuseToMixCurrencies() {
    Money euros = Money.of("10.00", "EUR");
    Money francs = Money.of("1.00", "CHF");

    assertThrows(IllegalArgumentException.class, () -> euros.plus(francs));
    assertThrows(IllegalArgumentException.class, () -> euros.minus(francs));
    assertThrows(IllegalArgumentException.class, () -> euros.isAtLeast(francs));
  }

  @Test
  void shouldNotLetAnAccountBeDebitedInAnotherCurrency() {
    Account account =
        new Account(UUID.randomUUID(), Money.of("100.00", "EUR"), AccountStatus.ACTIVE);

    assertThrows(IllegalArgumentException.class, () -> account.debit(Money.of("1.00", "CHF")));
    assertEquals(Money.of("100.00", "EUR"), account.getBalance());
  }
}
