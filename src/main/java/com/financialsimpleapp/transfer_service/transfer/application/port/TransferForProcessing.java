package com.financialsimpleapp.transfer_service.transfer.application.port;

import java.math.BigDecimal;
import java.util.UUID;

public record TransferForProcessing(
    UUID id,
    UUID sourceAccountId,
    UUID destinationAccountId,
    BigDecimal amount,
    String currency,
    String status) {
}
