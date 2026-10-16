package com.financialsimpleapp.transfer_service.transfer.api;

import com.financialsimpleapp.transfer_service.transfer.application.TransferAcceptanceResult;
import com.financialsimpleapp.transfer_service.transfer.application.TransferService;
import com.financialsimpleapp.transfer_service.transfer.application.TransferStatusResult;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Currency;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transfers")
public class TransferController {

    private final TransferService transferService;

    public TransferController(TransferService transferService) {
        this.transferService = transferService;
    }

    @PostMapping
    public ResponseEntity<TransferAcceptanceResponse> createTransfer(
            @RequestHeader("X-Client-Id") UUID clientId,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody TransferRequest request
    ) {
        Currency currency = Currency.getInstance(request.currency());

        TransferAcceptanceResult result =
                transferService.acceptTransfer(
                        clientId,
                        idempotencyKey,
                        request.fromAccountId(),
                        request.toAccountId(),
                        request.amount(),
                        currency
                );

        TransferAcceptanceResponse response =
                new TransferAcceptanceResponse(
                        result.transferId(),
                        result.status(),
                        result.replayed()
                );

        URI location = URI.create(
                "/api/v1/transfers/" + result.transferId()
        );

        return ResponseEntity.accepted()
                .location(location)
                .body(response);
    }

    @GetMapping("/{transferId}")
    public ResponseEntity<TransferStatusResponse> getTransfer(
            @PathVariable UUID transferId
    ) {
        TransferStatusResult result =
                transferService.getTransferStatus(transferId);

        return ResponseEntity.ok(
                new TransferStatusResponse(
                        result.transferId(),
                        result.status(),
                        result.rejectionCode(),
                        result.createdAt(),
                        result.completedAt()
                )
        );
    }
}