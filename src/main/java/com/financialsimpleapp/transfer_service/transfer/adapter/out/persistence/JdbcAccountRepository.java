package com.financialsimpleapp.transfer_service.transfer.adapter.out.persistence;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.AccountRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.Account;
import com.financialsimpleapp.transfer_service.transfer.domain.AccountStatus;
import com.financialsimpleapp.transfer_service.transfer.domain.Money;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcAccountRepository implements AccountRepository {

  private final JdbcTemplate jdbcTemplate;

  public JdbcAccountRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public List<Account> lockByIds(UUID firstAccountId, UUID secondAccountId) {
    return jdbcTemplate.query(
        """
        SELECT id, currency, balance, status
        FROM accounts
        WHERE id IN (?, ?)
        ORDER BY id
        FOR UPDATE
        """,
        (rs, rowNum) ->
            new Account(
                rs.getObject("id", UUID.class),
                new Money(
                    rs.getBigDecimal("balance"),
                    Currency.getInstance(rs.getString("currency").trim())),
                AccountStatus.valueOf(rs.getString("status"))),
        firstAccountId,
        secondAccountId);
  }

  @Override
  public void saveBalance(Account account) {
    // The row is locked by lockByIds, so writing the absolute
    // balance computed by the domain is safe.
    int updated =
        jdbcTemplate.update(
            """
            UPDATE accounts
            SET balance = ?, updated_at = now()
            WHERE id = ?
            """,
            account.getBalance().amount(),
            account.getId());

    if (updated != 1) {
      throw new IllegalStateException("Account balance could not be saved: " + account.getId());
    }
  }
}
