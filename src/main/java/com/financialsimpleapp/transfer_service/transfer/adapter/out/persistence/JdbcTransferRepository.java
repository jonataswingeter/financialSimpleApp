package com.financialsimpleapp.transfer_service.transfer.adapter.out.persistence;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferStatusData;
import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcTransferRepository implements TransferRepository {

  private final JdbcTemplate jdbcTemplate;

  public JdbcTransferRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public void insert(UUID clientId, Transfer transfer) {
    jdbcTemplate.update("""
        INSERT INTO transfer_requests (
            id,
            client_id,
            from_account_id,
            to_account_id,
            amount,
            currency,
            status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        transfer.getId(),
        clientId,
        transfer.getFromAccountId(),
        transfer.getToAccountId(),
        transfer.getAmount(),
        transfer.getCurrency().getCurrencyCode(),
        transfer.getStatus().name());
  }

  @Override
  public void deleteById(UUID transferId) {
    jdbcTemplate.update("""
        DELETE FROM transfer_requests
        WHERE id = ?
        """,
        transferId);
  }

  @Override
  public Optional<TransferStatusData> findStatusById(UUID transferId) {
    List<TransferStatusData> results = jdbcTemplate.query("""
        SELECT id, status, rejection_code, created_at, completed_at
        FROM transfer_requests
        WHERE id = ?
        """,
        (rs, rowNum) -> new TransferStatusData(
            rs.getObject("id", UUID.class),
            TransferStatus.valueOf(rs.getString("status")),
            rs.getString("rejection_code"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("completed_at") == null
                ? null
                : rs.getTimestamp("completed_at").toInstant()),
        transferId);

    return results.stream().findFirst();
  }
}