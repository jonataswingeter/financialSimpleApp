package com.financialsimpleapp.transfer_service.transfer.application.service;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.ProcessTransferUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.AccountRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.LedgerRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferRepository;
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
public class ProcessTransferService implements ProcessTransferUseCase {

  private final TransferRepository transferRepository;
  private final AccountRepository accountRepository;
  private final LedgerRepository ledgerRepository;

  public ProcessTransferService(
      TransferRepository transferRepository,
      AccountRepository accountRepository,
      LedgerRepository ledgerRepository) {
    this.transferRepository = transferRepository;
    this.accountRepository = accountRepository;
    this.ledgerRepository = ledgerRepository;
  }

  @Override
  @Transactional
  public void process(UUID transferId) {
    Transfer transfer =
        transferRepository
            .lockById(transferId)
            .orElseThrow(() -> new IllegalStateException("Transfer does not exist: " + transferId));

    // Kafka may deliver the same event more than once.
    if (transfer.isFinal()) {
      return;
    }

    Map<UUID, Account> accounts =
        accountRepository.lockByIds(transfer.getFromAccountId(), transfer.getToAccountId()).stream()
            .collect(Collectors.toMap(Account::getId, Function.identity()));

    Account source = accounts.get(transfer.getFromAccountId());
    Account destination = accounts.get(transfer.getToAccountId());

    if (source == null || destination == null) {
      transfer.rejectForMissingAccount();
      transferRepository.saveOutcome(transfer);
      return;
    }

    List<LedgerEntry> entries = transfer.execute(source, destination);

    if (transfer.getStatus() == TransferStatus.COMPLETED) {
      accountRepository.saveBalance(source);
      accountRepository.saveBalance(destination);
      entries.forEach(ledgerRepository::append);
    }

    transferRepository.saveOutcome(transfer);
  }
}
