package com.financialsimpleapp.transfer_service.transfer.application.port.out;

import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import java.util.Optional;
import java.util.UUID;

public interface TransferRepository {

  void insert(UUID clientId, Transfer transfer);

  void deleteById(UUID transferId);

  Optional<TransferStatusData> findStatusById(UUID transferId);
}
