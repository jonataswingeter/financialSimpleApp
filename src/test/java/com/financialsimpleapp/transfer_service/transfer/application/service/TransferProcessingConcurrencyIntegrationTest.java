package com.financialsimpleapp.transfer_service.transfer.application.service;

import static org.junit.jupiter.api.Assertions.*;

import com.financialsimpleapp.transfer_service.TestcontainersConfiguration;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferCommand;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.AcceptTransferUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.ProcessTransferUseCase;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Processes many transfers touching the same accounts in parallel, the way several consumer
 * instances or partitions would. The outbox relay is disabled (test default), so processing is
 * driven directly through the input port.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class TransferProcessingConcurrencyIntegrationTest {

  private static final Currency EUR = Currency.getInstance("EUR");

  // Below the default Hikari pool size, so every worker can hold a connection at once.
  private static final int WORKERS = 8;

  @Autowired private AcceptTransferUseCase acceptTransferUseCase;

  @Autowired private ProcessTransferUseCase processTransferUseCase;

  @Autowired private JdbcTemplate jdbcTemplate;

  private final UUID clientId = UUID.randomUUID();
  private final UUID accountA = UUID.randomUUID();
  private final UUID accountB = UUID.randomUUID();

  private final Map<UUID, BigDecimal> initialBalances =
      Map.of(accountA, new BigDecimal("1000.00"), accountB, new BigDecimal("500.00"));

  @BeforeEach
  void setUp() {
    jdbcTemplate.update("DELETE FROM outbox_events");
    jdbcTemplate.update("DELETE FROM ledger_entries");
    jdbcTemplate.update("DELETE FROM idempotency_records");
    jdbcTemplate.update("DELETE FROM transfer_requests");
    jdbcTemplate.update("DELETE FROM accounts");

    initialBalances.forEach(this::insertAccount);
  }

  @Test
  @Tag("scenario-03")
  @DisplayName("#03 Concurrent transfers: draining one account in parallel never overdraws it")
  void shouldNeverOverdrawWhenConcurrentTransfersDrainTheSameAccount() throws Exception {
    // 10 x 200.00 out of an account holding 1000.00: exactly half can succeed.
    List<UUID> transfers = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      transfers.add(accept("drain-" + i, accountA, accountB, "200.00"));
    }

    processConcurrently(transfers);

    assertEquals(5, countTransfers("COMPLETED", null));
    assertEquals(5, countTransfers("REJECTED", "INSUFFICIENT_FUNDS"));
    assertBalance(accountA, "0.00");
    assertBalance(accountB, "1500.00");
    assertLedgerIsConsistent();
  }

  @Test
  @Tag("scenario-03")
  @DisplayName("#03 Concurrent transfers: A->B and B->A in parallel never deadlock")
  void shouldNotDeadlockWhenTransfersCrossInOppositeDirections() throws Exception {
    // Interleaved A->B and B->A transfers lock the same two rows from both sides. Without a
    // consistent lock order, PostgreSQL would abort some of them with "deadlock detected".
    List<UUID> transfers = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      transfers.add(accept("a-to-b-" + i, accountA, accountB, "50.00"));
      transfers.add(accept("b-to-a-" + i, accountB, accountA, "30.00"));
    }

    processConcurrently(transfers);

    // Both sides always have enough funds, so every transfer completes in any order.
    assertEquals(20, countTransfers("COMPLETED", null));
    assertBalance(accountA, "800.00");
    assertBalance(accountB, "700.00");
    assertLedgerIsConsistent();
  }

  private UUID accept(String idempotencyKey, UUID from, UUID to, String amount) {
    return acceptTransferUseCase
        .acceptTransfer(
            new AcceptTransferCommand(
                clientId, idempotencyKey, from, to, new BigDecimal(amount), EUR))
        .transferId();
  }

  /** Starts all processing calls at the same moment and fails on the first exception. */
  private void processConcurrently(List<UUID> transferIds) throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> results = new ArrayList<>();

    try (ExecutorService executor = Executors.newFixedThreadPool(WORKERS)) {
      for (UUID transferId : transferIds) {
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  processTransferUseCase.process(transferId);
                  return null;
                }));
      }

      start.countDown();

      for (Future<?> result : results) {
        try {
          result.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException exception) {
          fail("Concurrent processing failed: " + exception.getCause(), exception.getCause());
        }
      }
    }
  }

  /**
   * Every completed transfer has exactly one debit and one credit, rejected ones have none, money
   * is conserved, and each balance equals its opening balance plus its ledger entries.
   */
  private void assertLedgerIsConsistent() {
    List<Map<String, Object>> entriesPerTransfer =
        jdbcTemplate.queryForList(
            """
            SELECT t.id, t.status, COUNT(l.id) AS entries
            FROM transfer_requests t
            LEFT JOIN ledger_entries l ON l.transfer_id = t.id
            GROUP BY t.id, t.status
            """);

    for (Map<String, Object> row : entriesPerTransfer) {
      long expected = "COMPLETED".equals(row.get("status")) ? 2 : 0;
      assertEquals(
          expected,
          ((Number) row.get("entries")).longValue(),
          "Ledger entries for " + row.get("status") + " transfer " + row.get("id"));
    }

    BigDecimal ledgerTotal =
        jdbcTemplate.queryForObject(
            "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries", BigDecimal.class);
    assertEquals(0, BigDecimal.ZERO.compareTo(ledgerTotal), "Money must be conserved");

    initialBalances.forEach(
        (accountId, opening) -> {
          BigDecimal ledgerSum =
              jdbcTemplate.queryForObject(
                  "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?",
                  BigDecimal.class,
                  accountId);
          BigDecimal balance =
              jdbcTemplate.queryForObject(
                  "SELECT balance FROM accounts WHERE id = ?", BigDecimal.class, accountId);

          assertEquals(
              0,
              opening.add(ledgerSum).compareTo(balance),
              "Balance of " + accountId + " does not match its ledger");
        });
  }

  private int countTransfers(String status, String rejectionCode) {
    return jdbcTemplate.queryForObject(
        """
        SELECT COUNT(*)
        FROM transfer_requests
        WHERE status = ?
          AND rejection_code IS NOT DISTINCT FROM ?
        """,
        Integer.class,
        status,
        rejectionCode);
  }

  private void assertBalance(UUID accountId, String expected) {
    BigDecimal balance =
        jdbcTemplate.queryForObject(
            "SELECT balance FROM accounts WHERE id = ?", BigDecimal.class, accountId);

    assertEquals(
        0,
        new BigDecimal(expected).compareTo(balance),
        "Unexpected balance for account " + accountId + ": " + balance);
  }

  private void insertAccount(UUID accountId, BigDecimal balance) {
    jdbcTemplate.update(
        """
        INSERT INTO accounts (id, currency, balance, status, created_at, updated_at)
        VALUES (?, 'EUR', ?, 'ACTIVE', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        accountId,
        balance);
  }
}
