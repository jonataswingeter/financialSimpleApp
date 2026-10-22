package com.financialsimpleapp.transfer_service.transfer.application.port;

import java.math.BigDecimal;
import java.util.UUID;

public record LockedAccount(
    UUID id,
    String currency,
    BigDecimal balance,
    String status) {
}
