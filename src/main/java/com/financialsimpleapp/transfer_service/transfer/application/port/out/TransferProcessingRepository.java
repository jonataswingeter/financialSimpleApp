package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.Account;
import com.financialsimpleapp.transfer_service.transfer.domain.LedgerEntry;
import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TransferProcessingRepository {

  /** Loads the transfer and locks it until the transaction ends. */
  Optional<Transfer> lockTransfer(UUID transferId);

  /** Loads the accounts that exist and locks them in a stable order. */
  List<Account> lockAccounts(
      UUID sourceAccountId,
      UUID destinationAccountId);

  /** Persists a COMPLETED or REJECTED transfer that was ACCEPTED. */
  void saveOutcome(Transfer transfer);

  void saveBalance(Account account);

  void insertLedgerEntry(LedgerEntry entry);
}
