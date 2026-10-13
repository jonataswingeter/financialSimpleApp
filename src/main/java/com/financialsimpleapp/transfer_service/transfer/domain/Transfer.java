package com.financialsimpleapp.transfer_service.transfer.domain;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Objects;
import java.util.UUID;

public class Transfer {

    private final UUID id;
    private final UUID fromAccountId;
    private final UUID toAccountId;
    private final BigDecimal amount;
    private final Currency currency;

    private TransferStatus status;
    private String rejectionCode;

    public Transfer(
            UUID id,
            UUID fromAccountId,
            UUID toAccountId,
            BigDecimal amount,
            Currency currency
    ) {
        this.id = Objects.requireNonNull(id, "Transfer ID is required");
        this.fromAccountId = Objects.requireNonNull(
                fromAccountId, "Source account ID is required");
        this.toAccountId = Objects.requireNonNull(
                toAccountId, "Destination account ID is required");
        this.amount = Objects.requireNonNull(amount, "Amount is required");
        this.currency = Objects.requireNonNull(currency, "Currency is required");

        if (fromAccountId.equals(toAccountId)) {
            throw new IllegalArgumentException(
                    "Source and destination accounts must be different");
        }

        if (amount.signum() <= 0) {
            throw new IllegalArgumentException(
                    "Transfer amount must be positive");
        }

        if (amount.scale() > 2) {
            throw new IllegalArgumentException(
                    "Transfer amount cannot have more than 2 decimal places");
        }

        this.status = TransferStatus.ACCEPTED;
    }

    public void startProcessing() {
        if (status != TransferStatus.ACCEPTED) {
            throw new IllegalStateException(
                    "Only accepted transfers can start processing");
        }

        status = TransferStatus.PROCESSING;
    }

    public void complete() {
        if (status != TransferStatus.PROCESSING) {
            throw new IllegalStateException(
                    "Only processing transfers can be completed");
        }

        status = TransferStatus.COMPLETED;
        rejectionCode = null;
    }

    public void reject(String rejectionCode) {
        if (status != TransferStatus.PROCESSING) {
            throw new IllegalStateException(
                    "Only processing transfers can be rejected");
        }

        if (rejectionCode == null || rejectionCode.isBlank()) {
            throw new IllegalArgumentException(
                    "Rejection code is required");
        }

        status = TransferStatus.REJECTED;
        this.rejectionCode = rejectionCode;
    }

    public UUID getId() {
        return id;
    }

    public UUID getFromAccountId() {
        return fromAccountId;
    }

    public UUID getToAccountId() {
        return toAccountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Currency getCurrency() {
        return currency;
    }

    public TransferStatus getStatus() {
        return status;
    }

    public String getRejectionCode() {
        return rejectionCode;
    }
}

