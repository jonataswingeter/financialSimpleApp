package com.financialsimpleapp.transfer_service.transfer.application.service;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.ProcessTransferUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferProcessingRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.Account;
import com.financialsimpleapp.transfer_service.transfer.domain.LedgerEntry;
import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransferProcessingService implements ProcessTransferUseCase {

  private final TransferProcessingRepository repository;

  public TransferProcessingService(TransferProcessingRepository repository) {
    this.repository = repository;
  }

  @Override
  @Transactional
  public void process(UUID transferId) {
    Transfer transfer =
        repository
            .lockTransfer(transferId)
            .orElseThrow(() -> new IllegalStateException("Transfer does not exist: " + transferId));

    // Kafka may deliver the same event more than once.
    if (transfer.isFinal()) {
      return;
    }

    Map<UUID, Account> accounts =
        repository.lockAccounts(transfer.getFromAccountId(), transfer.getToAccountId()).stream()
            .collect(Collectors.toMap(Account::getId, Function.identity()));

    Account source = accounts.get(transfer.getFromAccountId());
    Account destination = accounts.get(transfer.getToAccountId());

    if (source == null || destination == null) {
      transfer.rejectForMissingAccount();
      repository.saveOutcome(transfer);
      return;
    }

    List<LedgerEntry> entries = transfer.execute(source, destination);

    if (transfer.getStatus() == TransferStatus.COMPLETED) {
      repository.saveBalance(source);
      repository.saveBalance(destination);
      entries.forEach(repository::insertLedgerEntry);
    }

    repository.saveOutcome(transfer);
  }
}
