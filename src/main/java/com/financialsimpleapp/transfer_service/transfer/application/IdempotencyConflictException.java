package com.financialsimpleapp.transfer_service.transfer.application;

public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException() {
        super("Idempotency key was already used with a different request");
    }
}
