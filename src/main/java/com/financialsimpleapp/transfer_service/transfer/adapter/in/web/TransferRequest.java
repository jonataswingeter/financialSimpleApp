package com.financialsimpleapp.transfer_service.transfer.adapter.in.web;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;
import java.util.UUID;

public record TransferRequest(
        @NotNull
        UUID fromAccountId,

        @NotNull
        UUID toAccountId,

        @NotNull
        @DecimalMin(value = "0.00", inclusive = false)
        @Digits(integer = 17, fraction = 2)
        BigDecimal amount,

        @NotNull
        @Pattern(regexp = "^[A-Z]{3}$")
        String currency
) {
}