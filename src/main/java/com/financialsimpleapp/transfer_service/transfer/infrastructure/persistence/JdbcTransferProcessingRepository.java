package com.financialsimpleapp.transfer_service.transfer.infrastructure.persistence;

import com.financialsimpleapp.transfer_service.transfer.application.port.TransferProcessingRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.LockedAccount;
import com.financialsimpleapp.transfer_service.transfer.application.port.TransferForProcessing;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcTransferProcessingRepository
    implements TransferProcessingRepository {

  private final JdbcTemplate jdbcTemplate;

  public JdbcTransferProcessingRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public Optional<TransferForProcessing> lockTransfer(UUID transferId) {
    List<TransferForProcessing> rows = jdbcTemplate.query("""
        SELECT id, from_account_id, to_account_id,
               amount, currency, status
        FROM transfer_requests
        WHERE id = ?
        FOR UPDATE
        """,
        (rs, rowNum) -> new TransferForProcessing(
            rs.getObject("id", UUID.class),
            rs.getObject("from_account_id", UUID.class),
            rs.getObject("to_account_id", UUID.class),
            rs.getBigDecimal("amount"),
            rs.getString("currency").trim(),
            rs.getString("status")),
        transferId);

    return rows.stream().findFirst();
  }

  @Override
  public List<LockedAccount> lockAccounts(
      UUID sourceAccountId,
      UUID destinationAccountId) {

    return jdbcTemplate.query("""
        SELECT id, currency, balance, status
        FROM accounts
        WHERE id IN (?, ?)
        ORDER BY id
        FOR UPDATE
        """,
        (rs, rowNum) -> new LockedAccount(
            rs.getObject("id", UUID.class),
            rs.getString("currency").trim(),
            rs.getBigDecimal("balance"),
            rs.getString("status")),
        sourceAccountId,
        destinationAccountId);
  }

  @Override
  public void markProcessing(UUID transferId) {
    int updated = jdbcTemplate.update("""
        UPDATE transfer_requests
        SET status = 'PROCESSING', updated_at = now()
        WHERE id = ? AND status = 'ACCEPTED'
        """, transferId);

    if (updated != 1) {
      throw new IllegalStateException(
          "Transfer could not transition to PROCESSING: " + transferId);
    }
  }

  @Override
  public void reject(UUID transferId, String rejectionCode) {
    int updated = jdbcTemplate.update("""
        UPDATE transfer_requests
        SET status = 'REJECTED',
            rejection_code = ?,
            updated_at = now(),
            completed_at = NULL
        WHERE id = ? AND status = 'PROCESSING'
        """, rejectionCode, transferId);

    if (updated != 1) {
      throw new IllegalStateException(
          "Transfer could not be rejected: " + transferId);
    }
  }

  @Override
  public void debit(UUID accountId, BigDecimal amount) {
    int updated = jdbcTemplate.update("""
        UPDATE accounts
        SET balance = balance - ?, updated_at = now()
        WHERE id = ?
          AND status = 'ACTIVE'
          AND balance >= ?
        """, amount, accountId, amount);

    if (updated != 1) {
      throw new IllegalStateException(
          "Source account could not be debited: " + accountId);
    }
  }

  @Override
  public void credit(UUID accountId, BigDecimal amount) {
    int updated = jdbcTemplate.update("""
        UPDATE accounts
        SET balance = balance + ?, updated_at = now()
        WHERE id = ? AND status = 'ACTIVE'
        """, amount, accountId);

    if (updated != 1) {
      throw new IllegalStateException(
          "Destination account could not be credited: " + accountId);
    }
  }

  @Override
  public void insertLedgerEntry(
      UUID transferId,
      UUID accountId,
      String entryType,
      BigDecimal signedAmount,
      String currency) {

    jdbcTemplate.update("""
        INSERT INTO ledger_entries (
            id, transfer_id, account_id,
            entry_type, amount, currency, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, now())
        """,
        UUID.randomUUID(),
        transferId,
        accountId,
        entryType,
        signedAmount,
        currency);
  }

  @Override
  public void complete(UUID transferId) {
    int updated = jdbcTemplate.update("""
        UPDATE transfer_requests
        SET status = 'COMPLETED',
            rejection_code = NULL,
            completed_at = now(),
            updated_at = now()
        WHERE id = ? AND status = 'PROCESSING'
        """, transferId);

    if (updated != 1) {
      throw new IllegalStateException(
          "Transfer could not be completed: " + transferId);
    }
  }
}
