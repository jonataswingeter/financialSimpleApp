package com.financialsimpleapp.transfer_service.transfer.application.service;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferCommand;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.GetTransferStatusUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.IdempotencyConflictException;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferAcceptanceResult;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferStatusResult;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferNotFoundException;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.ExistingIdempotencyRecord;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.IdempotencyRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.OutboxRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferStatusData;
import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferAcceptedEvent;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

@Service
public class TransferService
    implements AcceptTransferUseCase, GetTransferStatusUseCase {

  private final TransferRepository transferRepository;
  private final IdempotencyRepository idempotencyRepository;
  private final OutboxRepository outboxRepository;

  public TransferService(
      TransferRepository transferRepository,
      IdempotencyRepository idempotencyRepository,
      OutboxRepository outboxRepository) {
    this.transferRepository = transferRepository;
    this.idempotencyRepository = idempotencyRepository;
    this.outboxRepository = outboxRepository;
  }

  @Override
  @Transactional
  public TransferAcceptanceResult acceptTransfer(
      AcceptTransferCommand command) {
    Objects.requireNonNull(command, "Command is required");

    UUID clientId = command.clientId();
    String idempotencyKey = command.idempotencyKey();

    Objects.requireNonNull(clientId, "Client ID is required");
    TransferSupport.validateIdempotencyKey(idempotencyKey);

    BigDecimal normalizedAmount = TransferSupport.normalizeAmount(
        command.amount());

    Transfer transfer = new Transfer(
        UUID.randomUUID(),
        command.fromAccountId(),
        command.toAccountId(),
        normalizedAmount,
        command.currency());

    String requestHash = TransferSupport.calculateRequestHash(
        transfer.getFromAccountId(),
        transfer.getToAccountId(),
        transfer.getAmount(),
        transfer.getCurrency().getCurrencyCode());

    // 1. Persist the candidate transfer.
    transferRepository.insert(clientId, transfer);

    // 2. Atomically reserve the idempotency key.
    boolean reserved = idempotencyRepository.tryCreate(
        clientId,
        idempotencyKey,
        requestHash,
        transfer.getId());

    if (!reserved) {
      // 3. Resolve the existing request.
      ExistingIdempotencyRecord existing = idempotencyRepository.find(clientId, idempotencyKey)
          .orElseThrow(() -> new IllegalStateException(
              "Idempotency record was not found after conflict"));

      if (!existing.requestHash().equals(requestHash)) {
        throw new IdempotencyConflictException();
      }

      // The existing transfer is authoritative.
      transferRepository.deleteById(transfer.getId());

      return new TransferAcceptanceResult(
          existing.transferId(),
          existing.status(),
          true);
    }

    // 4. Persist the event in the same database transaction.
    outboxRepository.insertTransferAccepted(
        new TransferAcceptedEvent(UUID.randomUUID(), transfer.getId()));

    return new TransferAcceptanceResult(
        transfer.getId(),
        TransferStatus.ACCEPTED,
        false);
  }

  @Override
  public TransferStatusResult getTransferStatus(
      UUID transferId) {
    Objects.requireNonNull(transferId, "Transfer ID is required");

    TransferStatusData result = transferRepository.findStatusById(transferId)
        .orElseThrow(() -> new TransferNotFoundException(transferId));
    return new TransferStatusResult(
        result.transferId(),
        result.status(),
        result.rejectionCode(),
        result.createdAt(),
        result.completedAt());
  }

}
