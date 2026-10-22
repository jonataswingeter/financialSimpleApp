package com.financialsimpleapp.transfer_service.transfer.application.port;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TransferProcessingRepository {

  Optional<TransferForProcessing> lockTransfer(UUID transferId);

  List<LockedAccount> lockAccounts(
      UUID sourceAccountId,
      UUID destinationAccountId);

  void markProcessing(UUID transferId);

  void reject(UUID transferId, String rejectionCode);

  void debit(UUID accountId, BigDecimal amount);

  void credit(UUID accountId, BigDecimal amount);

  void insertLedgerEntry(
      UUID transferId,
      UUID accountId,
      String entryType,
      BigDecimal signedAmount,
      String currency);

  void complete(UUID transferId);
}
