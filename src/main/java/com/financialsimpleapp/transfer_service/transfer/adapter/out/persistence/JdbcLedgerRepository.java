package com.financialsimpleapp.transfer_service.transfer.adapter.out.persistence;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.LedgerRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.LedgerEntry;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcLedgerRepository implements LedgerRepository {

  private final JdbcTemplate jdbcTemplate;

  public JdbcLedgerRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public void append(LedgerEntry entry) {
    jdbcTemplate.update(
        """
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
        entry.amount().amount(),
        entry.amount().currency().getCurrencyCode());
  }
}
