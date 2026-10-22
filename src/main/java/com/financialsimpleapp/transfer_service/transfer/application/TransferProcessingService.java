package com.financialsimpleapp.transfer_service.transfer.application;

import com.financialsimpleapp.transfer_service.transfer.application.port.TransferProcessingRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.LockedAccount;
import com.financialsimpleapp.transfer_service.transfer.application.port.TransferForProcessing;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransferProcessingService {

  private final TransferProcessingRepository repository;

  public TransferProcessingService(
      TransferProcessingRepository repository) {
    this.repository = repository;
  }

  @Transactional
  public void process(UUID transferId) {
    TransferForProcessing transfer = repository.lockTransfer(transferId)
        .orElseThrow(() -> new IllegalStateException(
            "Transfer does not exist: " + transferId));

    // Kafka may deliver the same event more than once.
    if ("COMPLETED".equals(transfer.status())
        || "REJECTED".equals(transfer.status())) {
      return;
    }

    if (!"ACCEPTED".equals(transfer.status())) {
      throw new IllegalStateException(
          "Unexpected transfer status " + transfer.status()
              + " for transfer " + transferId);
    }

    repository.markProcessing(transferId);

    Map<UUID, LockedAccount> accounts = repository.lockAccounts(
            transfer.sourceAccountId(),
            transfer.destinationAccountId())
        .stream()
        .collect(Collectors.toMap(
            LockedAccount::id,
            Function.identity()));

    LockedAccount source = accounts.get(transfer.sourceAccountId());
    LockedAccount destination = accounts.get(transfer.destinationAccountId());

    if (source == null || destination == null) {
      reject(transferId, "ACCOUNT_NOT_FOUND");
      return;
    }

    if (!"ACTIVE".equals(source.status())) {
      reject(transferId, "SOURCE_ACCOUNT_NOT_ACTIVE");
      return;
    }

    if (!"ACTIVE".equals(destination.status())) {
      reject(transferId, "DESTINATION_ACCOUNT_NOT_ACTIVE");
      return;
    }

    if (!transfer.currency().equals(source.currency())
        || !transfer.currency().equals(destination.currency())) {
      reject(transferId, "CURRENCY_MISMATCH");
      return;
    }

    BigDecimal amount = transfer.amount();

    if (source.balance().compareTo(amount) < 0) {
      reject(transferId, "INSUFFICIENT_FUNDS");
      return;
    }

    repository.debit(source.id(), amount);
    repository.credit(destination.id(), amount);

    repository.insertLedgerEntry(
        transferId,
        source.id(),
        "DEBIT",
        amount.negate(),
        transfer.currency());

    repository.insertLedgerEntry(
        transferId,
        destination.id(),
        "CREDIT",
        amount,
        transfer.currency());

    repository.complete(transferId);
  }

  private void reject(UUID transferId, String code) {
    repository.reject(transferId, code);
  }
}
