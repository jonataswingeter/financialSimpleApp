package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.AcceptedTransfer;
import com.financialsimpleapp.transfer_service.transfer.domain.ProcessableTransfer;
import java.util.Optional;
import java.util.UUID;

/** Persists the transfer aggregate, through the role each use case holds. */
public interface TransferRepository {

  void insert(AcceptedTransfer transfer);

  void deleteById(UUID transferId);

  /** Loads the transfer and locks it until the transaction ends. */
  Optional<ProcessableTransfer> lockById(UUID transferId);

  /** Persists a COMPLETED or REJECTED transfer that was ACCEPTED. */
  void saveOutcome(ProcessableTransfer transfer);

  /** Read model for the status endpoint; includes timestamps the aggregate doesn't carry. */
  Optional<TransferStatusData> findStatusById(UUID transferId);
}
