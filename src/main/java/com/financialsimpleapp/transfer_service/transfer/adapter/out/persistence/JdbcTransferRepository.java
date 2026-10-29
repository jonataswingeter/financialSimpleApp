package com.financialsimpleapp.transfer_service.transfer.adapter.out.persistence;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferStatusData;
import com.financialsimpleapp.transfer_service.transfer.domain.Money;
import com.financialsimpleapp.transfer_service.transfer.domain.RejectionReason;
import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcTransferRepository implements TransferRepository {

  private final JdbcTemplate jdbcTemplate;

  public JdbcTransferRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public void insert(Transfer transfer) {
    jdbcTemplate.update(
        """
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
        transfer.getClientId(),
        transfer.getFromAccountId(),
        transfer.getToAccountId(),
        transfer.getAmount().amount(),
        transfer.getAmount().currency().getCurrencyCode(),
        transfer.getStatus().name());
  }

  @Override
  public void deleteById(UUID transferId) {
    jdbcTemplate.update(
        """
        DELETE FROM transfer_requests
        WHERE id = ?
        """,
        transferId);
  }

  @Override
  public Optional<Transfer> lockById(UUID transferId) {
    List<Transfer> rows =
        jdbcTemplate.query(
            """
            SELECT id, client_id, from_account_id, to_account_id,
                   amount, currency, status, rejection_code
            FROM transfer_requests
            WHERE id = ?
            FOR UPDATE
            """,
            (rs, rowNum) -> {
              String rejectionCode = rs.getString("rejection_code");

              return Transfer.restore(
                  rs.getObject("id", UUID.class),
                  rs.getObject("client_id", UUID.class),
                  rs.getObject("from_account_id", UUID.class),
                  rs.getObject("to_account_id", UUID.class),
                  new Money(
                      rs.getBigDecimal("amount"),
                      Currency.getInstance(rs.getString("currency").trim())),
                  TransferStatus.valueOf(rs.getString("status")),
                  rejectionCode == null ? null : RejectionReason.valueOf(rejectionCode));
            },
            transferId);

    return rows.stream().findFirst();
  }

  @Override
  public void saveOutcome(Transfer transfer) {
    int updated =
        switch (transfer.getStatus()) {
          case COMPLETED ->
              jdbcTemplate.update(
                  """
                  UPDATE transfer_requests
                  SET status = 'COMPLETED',
                      rejection_code = NULL,
                      completed_at = now(),
                      updated_at = now()
                  WHERE id = ? AND status = 'ACCEPTED'
                  """,
                  transfer.getId());

          case REJECTED ->
              jdbcTemplate.update(
                  """
                  UPDATE transfer_requests
                  SET status = 'REJECTED',
                      rejection_code = ?,
                      completed_at = NULL,
                      updated_at = now()
                  WHERE id = ? AND status = 'ACCEPTED'
                  """,
                  transfer.getRejectionReason().name(),
                  transfer.getId());

          default ->
              throw new IllegalArgumentException(
                  "Transfer "
                      + transfer.getId()
                      + " has no final outcome: "
                      + transfer.getStatus());
        };

    if (updated != 1) {
      throw new IllegalStateException("Transfer outcome could not be saved: " + transfer.getId());
    }
  }

  @Override
  public Optional<TransferStatusData> findStatusById(UUID transferId) {
    List<TransferStatusData> results =
        jdbcTemplate.query(
            """
            SELECT id, status, rejection_code, created_at, completed_at
            FROM transfer_requests
            WHERE id = ?
            """,
            (rs, rowNum) ->
                new TransferStatusData(
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
