
package com.financialsimpleapp.transfer_service.transfer.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Currency;
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
}
