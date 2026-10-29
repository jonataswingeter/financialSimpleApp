package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import java.util.Optional;
import java.util.UUID;

/** Persists the {@link Transfer} aggregate. */
public interface TransferRepository {

  void insert(Transfer transfer);

  void deleteById(UUID transferId);

  /** Loads the transfer and locks it until the transaction ends. */
  Optional<Transfer> lockById(UUID transferId);

  /** Persists a COMPLETED or REJECTED transfer that was ACCEPTED. */
  void saveOutcome(Transfer transfer);

  /** Read model for the status endpoint; includes timestamps the aggregate doesn't carry. */
  Optional<TransferStatusData> findStatusById(UUID transferId);
}
