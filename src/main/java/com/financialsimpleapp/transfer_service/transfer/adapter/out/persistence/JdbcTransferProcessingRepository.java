package com.financialsimpleapp.transfer_service.transfer.adapter.out.persistence;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferProcessingRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.Account;
import com.financialsimpleapp.transfer_service.transfer.domain.AccountStatus;
import com.financialsimpleapp.transfer_service.transfer.domain.LedgerEntry;
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
public class JdbcTransferProcessingRepository
    implements TransferProcessingRepository {

  private final JdbcTemplate jdbcTemplate;

  public JdbcTransferProcessingRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public Optional<Transfer> lockTransfer(UUID transferId) {
    List<Transfer> rows = jdbcTemplate.query("""
        SELECT id, from_account_id, to_account_id,
               amount, currency, status, rejection_code
        FROM transfer_requests
        WHERE id = ?
        FOR UPDATE
        """,
        (rs, rowNum) -> {
          String rejectionCode = rs.getString("rejection_code");

          return Transfer.restore(
              rs.getObject("id", UUID.class),
              rs.getObject("from_account_id", UUID.class),
              rs.getObject("to_account_id", UUID.class),
              rs.getBigDecimal("amount"),
              Currency.getInstance(rs.getString("currency").trim()),
              TransferStatus.valueOf(rs.getString("status")),
              rejectionCode == null
                  ? null
                  : RejectionReason.valueOf(rejectionCode));
        },
        transferId);

    return rows.stream().findFirst();
  }

  @Override
  public List<Account> lockAccounts(
      UUID sourceAccountId,
      UUID destinationAccountId) {

    return jdbcTemplate.query("""
        SELECT id, currency, balance, status
        FROM accounts
        WHERE id IN (?, ?)
        ORDER BY id
        FOR UPDATE
        """,
        (rs, rowNum) -> new Account(
            rs.getObject("id", UUID.class),
            Currency.getInstance(rs.getString("currency").trim()),
            rs.getBigDecimal("balance"),
            AccountStatus.valueOf(rs.getString("status"))),
        sourceAccountId,
        destinationAccountId);
  }

  @Override
  public void saveOutcome(Transfer transfer) {
    int updated = switch (transfer.getStatus()) {
      case COMPLETED -> jdbcTemplate.update("""
          UPDATE transfer_requests
          SET status = 'COMPLETED',
              rejection_code = NULL,
              completed_at = now(),
              updated_at = now()
          WHERE id = ? AND status = 'ACCEPTED'
          """, transfer.getId());

      case REJECTED -> jdbcTemplate.update("""
          UPDATE transfer_requests
          SET status = 'REJECTED',
              rejection_code = ?,
              completed_at = NULL,
              updated_at = now()
          WHERE id = ? AND status = 'ACCEPTED'
          """, transfer.getRejectionReason().name(), transfer.getId());

      default -> throw new IllegalArgumentException(
          "Transfer " + transfer.getId() + " has no final outcome: "
              + transfer.getStatus());
    };

    if (updated != 1) {
      throw new IllegalStateException(
          "Transfer outcome could not be saved: " + transfer.getId());
    }
  }

  @Override
  public void saveBalance(Account account) {
    // The row is locked by lockAccounts, so writing the absolute
    // balance computed by the domain is safe.
    int updated = jdbcTemplate.update("""
        UPDATE accounts
        SET balance = ?, updated_at = now()
        WHERE id = ?
        """, account.getBalance(), account.getId());

    if (updated != 1) {
      throw new IllegalStateException(
          "Account balance could not be saved: " + account.getId());
    }
  }

  @Override
  public void insertLedgerEntry(LedgerEntry entry) {
    jdbcTemplate.update("""
        INSERT INTO ledger_entries (
            id, transfer_id, account_id,
            entry_type, amount, currency, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, now())
        """,
        UUID.randomUUID(),
        entry.transferId(),
        entry.accountId(),
        entry.type().name(),
        entry.amount(),
        entry.currency().getCurrencyCode());
  }
}
