package com.financialsimpleapp.transfer_service.transfer.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TransferTest {

  private static final UUID CLIENT_ID = UUID.randomUUID();

  private Transfer newTransfer(String amount) {
    return Transfer.accept(
        UUID.randomUUID(),
        CLIENT_ID,
        UUID.randomUUID(),
        UUID.randomUUID(),
        Money.of(amount, "CHF"));
  }

  @Nested
  class Accept {

    @Test
    void shouldStartInAcceptedStatus() {
      Transfer transfer = newTransfer("100.00");

      assertEquals(TransferStatus.ACCEPTED, transfer.getStatus());
      assertEquals(CLIENT_ID, transfer.getClientId());
    }

    @Test
    void shouldRecordTransferAcceptedEvent() {
      Transfer transfer = newTransfer("100.00");

      List<TransferEvent> events = transfer.pullEvents();

      assertEquals(1, events.size());
      TransferAcceptedEvent event =
          assertInstanceOf(TransferAcceptedEvent.class, events.getFirst());
      assertEquals(transfer.getId(), event.transferId());
      assertEquals(TransferAcceptedEvent.TYPE, event.type());
    }

    @Test
    void shouldHandOutEventsOnlyOnce() {
      Transfer transfer = newTransfer("100.00");

      transfer.pullEvents();

      assertTrue(transfer.pullEvents().isEmpty());
    }

    @Test
    void shouldRejectNegativeAmount() {
      assertThrows(IllegalArgumentException.class, () -> newTransfer("-10.00"));
    }

    @Test
    void shouldRejectZeroAmount() {
      assertThrows(IllegalArgumentException.class, () -> newTransfer("0.00"));
    }

    @Test
    void shouldRejectTransferBetweenSameAccount() {
      UUID accountId = UUID.randomUUID();

      assertThrows(
          IllegalArgumentException.class,
          () ->
              Transfer.accept(
                  UUID.randomUUID(), CLIENT_ID, accountId, accountId, Money.of("10.00", "CHF")));
    }
  }

  @Nested
  class Execute {

    private final UUID sourceId = UUID.randomUUID();
    private final UUID destinationId = UUID.randomUUID();

    private Transfer transfer(String amount, String currency) {
      return Transfer.accept(
          UUID.randomUUID(), CLIENT_ID, sourceId, destinationId, Money.of(amount, currency));
    }

    private Account account(UUID id, String balance, String currency, AccountStatus status) {
      return new Account(id, Money.of(balance, currency), status);
    }

    @Test
    void shouldMoveMoneyAndProduceLedgerEntries() {
      Transfer transfer = transfer("25.00", "EUR");
      Account source = account(sourceId, "100.00", "EUR", AccountStatus.ACTIVE);
      Account destination = account(destinationId, "10.00", "EUR", AccountStatus.ACTIVE);

      List<LedgerEntry> entries = transfer.execute(source, destination);

      assertEquals(TransferStatus.COMPLETED, transfer.getStatus());
      assertNull(transfer.getRejectionReason());
      assertTrue(transfer.isFinal());
      assertEquals(Money.of("75.00", "EUR"), source.getBalance());
      assertEquals(Money.of("35.00", "EUR"), destination.getBalance());

      assertEquals(
          List.of(
              LedgerEntry.debit(transfer.getId(), sourceId, Money.of("25.00", "EUR")),
              LedgerEntry.credit(transfer.getId(), destinationId, Money.of("25.00", "EUR"))),
          entries);
    }

    @Test
    void shouldAllowTransferOfWholeBalance() {
      Transfer transfer = transfer("100.00", "EUR");
      Account source = account(sourceId, "100.00", "EUR", AccountStatus.ACTIVE);
      Account destination = account(destinationId, "0.00", "EUR", AccountStatus.ACTIVE);

      transfer.execute(source, destination);

      assertEquals(TransferStatus.COMPLETED, transfer.getStatus());
      assertEquals(Money.of("0.00", "EUR"), source.getBalance());
    }

    @Test
    void shouldRejectWhenSourceIsNotActive() {
      assertRejected(
          transfer("25.00", "EUR"),
          account(sourceId, "100.00", "EUR", AccountStatus.BLOCKED),
          account(destinationId, "10.00", "EUR", AccountStatus.ACTIVE),
          RejectionReason.SOURCE_ACCOUNT_NOT_ACTIVE);
    }

    @Test
    void shouldRejectWhenDestinationIsNotActive() {
      assertRejected(
          transfer("25.00", "EUR"),
          account(sourceId, "100.00", "EUR", AccountStatus.ACTIVE),
          account(destinationId, "10.00", "EUR", AccountStatus.CLOSED),
          RejectionReason.DESTINATION_ACCOUNT_NOT_ACTIVE);
    }

    @Test
    void shouldRejectWhenCurrencyDoesNotMatch() {
      assertRejected(
          transfer("25.00", "EUR"),
          account(sourceId, "100.00", "EUR", AccountStatus.ACTIVE),
          account(destinationId, "10.00", "CHF", AccountStatus.ACTIVE),
          RejectionReason.CURRENCY_MISMATCH);
    }

    @Test
    void shouldRejectWhenFundsAreInsufficient() {
      assertRejected(
          transfer("100.01", "EUR"),
          account(sourceId, "100.00", "EUR", AccountStatus.ACTIVE),
          account(destinationId, "10.00", "EUR", AccountStatus.ACTIVE),
          RejectionReason.INSUFFICIENT_FUNDS);
    }

    @Test
    void shouldRejectWhenAnAccountIsMissing() {
      Transfer transfer = transfer("25.00", "EUR");

      transfer.rejectForMissingAccount();

      assertEquals(TransferStatus.REJECTED, transfer.getStatus());
      assertEquals(RejectionReason.ACCOUNT_NOT_FOUND, transfer.getRejectionReason());
    }

    @Test
    void shouldNotExecuteTwice() {
      Transfer transfer = transfer("25.00", "EUR");
      Account source = account(sourceId, "100.00", "EUR", AccountStatus.ACTIVE);
      Account destination = account(destinationId, "10.00", "EUR", AccountStatus.ACTIVE);

      transfer.execute(source, destination);

      assertThrows(IllegalStateException.class, () -> transfer.execute(source, destination));
      assertEquals(Money.of("75.00", "EUR"), source.getBalance());
    }

    @Test
    void shouldNotRejectAFinishedTransfer() {
      Transfer transfer = transfer("25.00", "EUR");
      transfer.execute(
          account(sourceId, "100.00", "EUR", AccountStatus.ACTIVE),
          account(destinationId, "10.00", "EUR", AccountStatus.ACTIVE));

      assertThrows(IllegalStateException.class, transfer::rejectForMissingAccount);
      assertEquals(TransferStatus.COMPLETED, transfer.getStatus());
    }

    @Test
    void shouldRefuseAccountsOfAnotherTransfer() {
      Transfer transfer = transfer("25.00", "EUR");
      Account stranger = account(UUID.randomUUID(), "100.00", "EUR", AccountStatus.ACTIVE);
      Account destination = account(destinationId, "10.00", "EUR", AccountStatus.ACTIVE);

      assertThrows(IllegalArgumentException.class, () -> transfer.execute(stranger, destination));
      assertEquals(TransferStatus.ACCEPTED, transfer.getStatus());
    }

    private void assertRejected(
        Transfer transfer, Account source, Account destination, RejectionReason expectedReason) {
      Money sourceBalance = source.getBalance();
      Money destinationBalance = destination.getBalance();

      List<LedgerEntry> entries = transfer.execute(source, destination);

      assertEquals(TransferStatus.REJECTED, transfer.getStatus());
      assertEquals(expectedReason, transfer.getRejectionReason());
      assertTrue(entries.isEmpty());
      assertEquals(sourceBalance, source.getBalance());
      assertEquals(destinationBalance, destination.getBalance());
    }
  }

  @Nested
  class Restore {

    private Transfer restore(TransferStatus status, RejectionReason reason) {
      return Transfer.restore(
          UUID.randomUUID(),
          CLIENT_ID,
          UUID.randomUUID(),
          UUID.randomUUID(),
          Money.of("10.00", "EUR"),
          status,
          reason);
    }

    @Test
    void shouldRestorePersistedTransfer() {
      Transfer transfer = restore(TransferStatus.REJECTED, RejectionReason.INSUFFICIENT_FUNDS);

      assertTrue(transfer.isFinal());
      assertEquals(RejectionReason.INSUFFICIENT_FUNDS, transfer.getRejectionReason());
    }

    @Test
    void shouldNotRecordEventsWhenRestoring() {
      assertTrue(restore(TransferStatus.ACCEPTED, null).pullEvents().isEmpty());
    }

    @Test
    void shouldNotRestoreRejectedTransferWithoutReason() {
      assertThrows(IllegalArgumentException.class, () -> restore(TransferStatus.REJECTED, null));
    }
  }
}
