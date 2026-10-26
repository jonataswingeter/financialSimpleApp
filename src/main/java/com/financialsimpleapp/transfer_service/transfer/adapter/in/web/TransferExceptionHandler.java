package com.financialsimpleapp.transfer_service.transfer.adapter.in.web;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.IdempotencyConflictException;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class TransferExceptionHandler {

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<Map<String, String>> handleIdempotencyConflict(
            IdempotencyConflictException exception
    ) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of(
                        "code", "IDEMPOTENCY_KEY_REUSED",
                        "message", exception.getMessage()
                ));
    }

    @ExceptionHandler(TransferNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleNotFound(
            TransferNotFoundException exception
    ) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of(
                        "code", "TRANSFER_NOT_FOUND",
                        "message", exception.getMessage()
                ));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> handleValidation(
            MethodArgumentNotValidException exception
    ) {
        String message = exception.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(error -> error.getField() + ": "
                        + error.getDefaultMessage())
                .findFirst()
                .orElse("Request validation failed");

        return ResponseEntity.badRequest()
                .body(Map.of(
                        "code", "VALIDATION_ERROR",
                        "message", message
                ));
    }

    @ExceptionHandler({
            IllegalArgumentException.class,
            HttpMessageNotReadableException.class
    })
    public ResponseEntity<Map<String, String>> handleBadRequest(
            Exception exception
    ) {
        return ResponseEntity.badRequest()
                .body(Map.of(
                        "code", "BAD_REQUEST",
                        "message", "The request contains invalid data"
                ));
    }
}