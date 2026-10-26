package com.financialsimpleapp.transfer_service.transfer.application;

import com.financialsimpleapp.transfer_service.transfer.application.port.ExistingIdempotencyRecord;
import com.financialsimpleapp.transfer_service.transfer.application.port.IdempotencyRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.OutboxRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.TransferRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.TransferStatusData;
import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Objects;
import java.util.UUID;

@Service
public class TransferService {

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

  @Transactional
  public TransferAcceptanceResult acceptTransfer(
      UUID clientId,
      String idempotencyKey,
      UUID fromAccountId,
      UUID toAccountId,
      BigDecimal amount,
      Currency currency) {
    Objects.requireNonNull(clientId, "Client ID is required");
    TransferSupport.validateIdempotencyKey(idempotencyKey);

    BigDecimal normalizedAmount = TransferSupport.normalizeAmount(amount);

    Transfer transfer = new Transfer(
        UUID.randomUUID(),
        fromAccountId,
        toAccountId,
        normalizedAmount,
        currency);

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
        UUID.randomUUID(),
        transfer.getId());

    return new TransferAcceptanceResult(
        transfer.getId(),
        TransferStatus.ACCEPTED,
        false);
  }

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
