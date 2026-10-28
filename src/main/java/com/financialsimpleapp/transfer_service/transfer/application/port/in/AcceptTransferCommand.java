package com.financialsimpleapp.transfer_service.transfer.application.port.in;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.UUID;

public record AcceptTransferCommand(
    UUID clientId,
    String idempotencyKey,
    UUID fromAccountId,
    UUID toAccountId,
    BigDecimal amount,
    Currency currency) {}
