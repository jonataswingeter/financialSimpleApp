package com.financialsimpleapp.transfer_service.transfer.application.port;

import java.util.Optional;
import java.util.UUID;

import com.financialsimpleapp.transfer_service.transfer.infrastructure.persistence.JdbcIdempotencyRepository.ExistingIdempotencyRecord;

public interface IdempotencyRepository {

  boolean tryCreate(
      UUID clientId,
      String idempotencyKey,
      String requestHash,
      UUID transferId);

  Optional<ExistingIdempotencyRecord> find(
      UUID clientId,
      String idempotencyKey);

}