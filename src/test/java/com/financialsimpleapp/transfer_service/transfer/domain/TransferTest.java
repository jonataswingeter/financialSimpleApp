
package com.financialsimpleapp.transfer_service.transfer.domain;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TransferTest {

    private Transfer newTransfer(BigDecimal amount) {
        return new Transfer(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                amount,
                Currency.getInstance("CHF")
        );
    }

    @Test
    void shouldStartInAcceptedStatus() {
        Transfer transfer = newTransfer(new BigDecimal("100.00"));

        assertEquals(TransferStatus.ACCEPTED, transfer.getStatus());
    }

    @Test
    void shouldCompleteTransferAfterProcessing() {
        Transfer transfer = newTransfer(new BigDecimal("100.00"));

        transfer.startProcessing();
        transfer.complete();

        assertEquals(TransferStatus.COMPLETED, transfer.getStatus());
    }

    @Test
    void shouldRejectNegativeAmount() {
        assertThrows(
                IllegalArgumentException.class,
                () -> newTransfer(new BigDecimal("-10.00"))
        );
    }

    @Test
    void shouldRejectTransferBetweenSameAccount() {
        UUID accountId = UUID.randomUUID();

        assertThrows(
                IllegalArgumentException.class,
                () -> new Transfer(
                        UUID.randomUUID(),
                        accountId,
                        accountId,
                        new BigDecimal("10.00"),
                        Currency.getInstance("CHF")
                )
        );
    }

    @Test
    void shouldNotCompleteTransferBeforeProcessing() {
        Transfer transfer = newTransfer(new BigDecimal("100.00"));

        assertThrows(
                IllegalStateException.class,
                transfer::complete
        );
    }

    @Nested
    class Execute {

        private final UUID sourceId = UUID.randomUUID();
        private final UUID destinationId = UUID.randomUUID();

        private Transfer transfer(String amount, String currency) {
            return new Transfer(
                    UUID.randomUUID(),
                    sourceId,
                    destinationId,
                    new BigDecimal(amount),
                    Currency.getInstance(currency)
            );
        }

        private Account account(
                UUID id,
                String balance,
                String currency,
                AccountStatus status
        ) {
            return new Account(
                    id,
                    Currency.getInstance(currency),
                    new BigDecimal(balance),
                    status
            );
        }

        @Test
        void shouldMoveMoneyAndProduceLedgerEntries() {
            Transfer transfer = transfer("25.00", "EUR");
            Account source = account(
                    sourceId, "100.00", "EUR", AccountStatus.ACTIVE);
            Account destination = account(
                    destinationId, "10.00", "EUR", AccountStatus.ACTIVE);

            List<LedgerEntry> entries = transfer.execute(source, destination);

            assertEquals(TransferStatus.COMPLETED, transfer.getStatus());
            assertNull(transfer.getRejectionReason());
            assertTrue(transfer.isFinal());
            assertEquals(new BigDecimal("75.00"), source.getBalance());
            assertEquals(new BigDecimal("35.00"), destination.getBalance());

            assertEquals(
                    List.of(
                            LedgerEntry.debit(transfer.getId(), sourceId,
                                    new BigDecimal("25.00"),
                                    Currency.getInstance("EUR")),
                            LedgerEntry.credit(transfer.getId(), destinationId,
                                    new BigDecimal("25.00"),
                                    Currency.getInstance("EUR"))),
                    entries);
        }

        @Test
        void shouldAllowTransferOfWholeBalance() {
            Transfer transfer = transfer("100.00", "EUR");
            Account source = account(
                    sourceId, "100.00", "EUR", AccountStatus.ACTIVE);
            Account destination = account(
                    destinationId, "0.00", "EUR", AccountStatus.ACTIVE);

            transfer.execute(source, destination);

            assertEquals(TransferStatus.COMPLETED, transfer.getStatus());
            assertEquals(0, BigDecimal.ZERO.compareTo(source.getBalance()));
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
            assertEquals(
                    RejectionReason.ACCOUNT_NOT_FOUND,
                    transfer.getRejectionReason());
        }

        @Test
        void shouldNotExecuteTwice() {
            Transfer transfer = transfer("25.00", "EUR");
            Account source = account(
                    sourceId, "100.00", "EUR", AccountStatus.ACTIVE);
            Account destination = account(
                    destinationId, "10.00", "EUR", AccountStatus.ACTIVE);

            transfer.execute(source, destination);

            assertThrows(
                    IllegalStateException.class,
                    () -> transfer.execute(source, destination));
            assertEquals(new BigDecimal("75.00"), source.getBalance());
        }

        @Test
        void shouldRefuseAccountsOfAnotherTransfer() {
            Transfer transfer = transfer("25.00", "EUR");
            Account stranger = account(
                    UUID.randomUUID(), "100.00", "EUR", AccountStatus.ACTIVE);
            Account destination = account(
                    destinationId, "10.00", "EUR", AccountStatus.ACTIVE);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> transfer.execute(stranger, destination));
            assertEquals(TransferStatus.ACCEPTED, transfer.getStatus());
        }

        private void assertRejected(
                Transfer transfer,
                Account source,
                Account destination,
                RejectionReason expectedReason
        ) {
            BigDecimal sourceBalance = source.getBalance();
            BigDecimal destinationBalance = destination.getBalance();

            List<LedgerEntry> entries = transfer.execute(source, destination);

            assertEquals(TransferStatus.REJECTED, transfer.getStatus());
            assertEquals(expectedReason, transfer.getRejectionReason());
            assertTrue(entries.isEmpty());
            assertEquals(sourceBalance, source.getBalance());
            assertEquals(destinationBalance, destination.getBalance());
        }
    }

    @Test
    void shouldRestorePersistedTransfer() {
        Transfer transfer = Transfer.restore(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("10.00"),
                Currency.getInstance("EUR"),
                TransferStatus.REJECTED,
                RejectionReason.INSUFFICIENT_FUNDS
        );

        assertTrue(transfer.isFinal());
        assertEquals(
                RejectionReason.INSUFFICIENT_FUNDS,
                transfer.getRejectionReason());
    }

    @Test
    void shouldNotRestoreRejectedTransferWithoutReason() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Transfer.restore(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        new BigDecimal("10.00"),
                        Currency.getInstance("EUR"),
                        TransferStatus.REJECTED,
                        null
                )
        );
    }
}
