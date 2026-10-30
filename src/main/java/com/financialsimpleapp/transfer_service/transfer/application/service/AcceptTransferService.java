package com.financialsimpleapp.transfer_service.transfer.application.service;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferCommand;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.IdempotencyConflictException;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.TransferAcceptanceResult;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.ExistingIdempotencyRecord;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.IdempotencyRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.OutboxRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.AcceptedTransfer;
import com.financialsimpleapp.transfer_service.transfer.domain.Money;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AcceptTransferService implements AcceptTransferUseCase {

  private final TransferRepository transferRepository;
  private final IdempotencyRepository idempotencyRepository;
  private final OutboxRepository outboxRepository;

  public AcceptTransferService(
      TransferRepository transferRepository,
      IdempotencyRepository idempotencyRepository,
      OutboxRepository outboxRepository) {
    this.transferRepository = transferRepository;
    this.idempotencyRepository = idempotencyRepository;
    this.outboxRepository = outboxRepository;
  }

  @Override
  @Transactional
  public TransferAcceptanceResult acceptTransfer(AcceptTransferCommand command) {
    Objects.requireNonNull(command, "Command is required");

    UUID clientId = command.clientId();
    String idempotencyKey = command.idempotencyKey();

    Objects.requireNonNull(clientId, "Client ID is required");
    TransferSupport.validateIdempotencyKey(idempotencyKey);

    AcceptedTransfer transfer =
        AcceptedTransfer.accept(
            UUID.randomUUID(),
            clientId,
            command.fromAccountId(),
            command.toAccountId(),
            new Money(command.amount(), command.currency()));

    Money amount = transfer.getAmount();
    String requestHash =
        TransferSupport.calculateRequestHash(
            transfer.getFromAccountId(),
            transfer.getToAccountId(),
            amount.amount(),
            amount.currency().getCurrencyCode());

    // 1. Persist the candidate transfer.
    transferRepository.insert(transfer);

    // 2. Atomically reserve the idempotency key.
    boolean reserved =
        idempotencyRepository.tryCreate(clientId, idempotencyKey, requestHash, transfer.getId());

    if (!reserved) {
      // 3. Resolve the existing request.
      ExistingIdempotencyRecord existing =
          idempotencyRepository
              .find(clientId, idempotencyKey)
              .orElseThrow(
                  () ->
                      new IllegalStateException("Idempotency record was not found after conflict"));

      if (!existing.requestHash().equals(requestHash)) {
        throw new IdempotencyConflictException();
      }

      // The existing transfer is authoritative.
      transferRepository.deleteById(transfer.getId());

      return new TransferAcceptanceResult(existing.transferId(), existing.status(), true);
    }

    // 4. Store the events the transfer recorded, in the same database transaction.
    transfer.pullEvents().forEach(outboxRepository::append);

    return new TransferAcceptanceResult(transfer.getId(), TransferStatus.ACCEPTED, false);
  }
}
