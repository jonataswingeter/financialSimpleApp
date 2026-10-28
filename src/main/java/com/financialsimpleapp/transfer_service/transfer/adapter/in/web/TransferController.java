package com.financialsimpleapp.transfer_service.transfer.adapter.in.web;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferCommand;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.GetTransferStatusUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferAcceptanceResult;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferStatusResult;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.Currency;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transfers")
public class TransferController {

  private final AcceptTransferUseCase acceptTransferUseCase;
  private final GetTransferStatusUseCase getTransferStatusUseCase;

  public TransferController(
      AcceptTransferUseCase acceptTransferUseCase,
      GetTransferStatusUseCase getTransferStatusUseCase) {
    this.acceptTransferUseCase = acceptTransferUseCase;
    this.getTransferStatusUseCase = getTransferStatusUseCase;
  }

  @PostMapping
  public ResponseEntity<TransferAcceptanceResponse> createTransfer(
      @RequestHeader("X-Client-Id") UUID clientId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @Valid @RequestBody TransferRequest request) {
    Currency currency = Currency.getInstance(request.currency());

    TransferAcceptanceResult result =
        acceptTransferUseCase.acceptTransfer(
            new AcceptTransferCommand(
                clientId,
                idempotencyKey,
                request.fromAccountId(),
                request.toAccountId(),
                request.amount(),
                currency));

    TransferAcceptanceResponse response =
        new TransferAcceptanceResponse(result.transferId(), result.status(), result.replayed());

    URI location = URI.create("/api/v1/transfers/" + result.transferId());

    return ResponseEntity.accepted().location(location).body(response);
  }

  @GetMapping("/{transferId}")
  public ResponseEntity<TransferStatusResponse> getTransfer(@PathVariable UUID transferId) {
    TransferStatusResult result = getTransferStatusUseCase.getTransferStatus(transferId);

    return ResponseEntity.ok(
        new TransferStatusResponse(
            result.transferId(),
            result.status(),
            result.rejectionCode(),
            result.createdAt(),
            result.completedAt()));
  }
}
