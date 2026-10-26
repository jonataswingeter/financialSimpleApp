package com.financialsimpleapp.transfer_service.transfer.domain;

public enum RejectionReason {
    ACCOUNT_NOT_FOUND,
    SOURCE_ACCOUNT_NOT_ACTIVE,
    DESTINATION_ACCOUNT_NOT_ACTIVE,
    CURRENCY_MISMATCH,
    INSUFFICIENT_FUNDS
}
