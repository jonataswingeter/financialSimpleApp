package com.financialsimpleapp.transfer_service.transfer.application;

import java.util.concurrent.*;

import com.financialsimpleapp.transfer_service.TestcontainersConfiguration;
import com.financialsimpleapp.transfer_service.transfer.application.port.OutboxRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class TransferServiceIntegrationTest {

  @Autowired
  private TransferService transferService;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @MockitoSpyBean
  private OutboxRepository outboxRepository;

  private final UUID clientId = UUID.randomUUID();
  private final UUID fromAccountId = UUID.randomUUID();
  private final UUID toAccountId = UUID.randomUUID();

  private static final Currency EUR = Currency.getInstance("EUR");

  @BeforeEach
  void setUp() {
    reset(outboxRepository);

    jdbcTemplate.update("DELETE FROM outbox_events");
    jdbcTemplate.update("DELETE FROM ledger_entries");
    jdbcTemplate.update("DELETE FROM idempotency_records");
    jdbcTemplate.update("DELETE FROM transfer_requests");
    jdbcTemplate.update("DELETE FROM accounts");

    insertAccount(fromAccountId, new BigDecimal("1000.00"));
    insertAccount(toAccountId, new BigDecimal("500.00"));
  }

  @Test
  void shouldCreateTransferIdempotencyRecordAndOutboxEvent() {
    TransferAcceptanceResult result = accept(
        "request-001",
        new BigDecimal("25.00"));

    assertNotNull(result.transferId());
    assertEquals(TransferStatus.ACCEPTED, result.status());
    assertFalse(result.replayed());

    assertEquals(1, countTransfers());
    assertEquals(1, countIdempotencyRecords());
    assertEquals(1, countOutboxEvents());

    String storedStatus = jdbcTemplate.queryForObject(
        "SELECT status FROM transfer_requests WHERE id = ?",
        String.class,
        result.transferId());

    assertEquals("ACCEPTED", storedStatus);
  }

  @Test
  void shouldReturnOriginalTransferForIdenticalRetry() {
    TransferAcceptanceResult first = accept(
        "request-002",
        new BigDecimal("25.00"));

    TransferAcceptanceResult retry = accept(
        "request-002",
        new BigDecimal("25.00"));

    assertEquals(first.transferId(), retry.transferId());
    assertEquals(TransferStatus.ACCEPTED, retry.status());
    assertFalse(first.replayed());
    assertTrue(retry.replayed());

    assertEquals(1, countTransfers());
    assertEquals(1, countIdempotencyRecords());
    assertEquals(1, countOutboxEvents());
  }

  @Test
  void shouldRejectSameKeyWithDifferentRequestAndRollbackCandidate() {
    TransferAcceptanceResult first = accept(
        "request-003",
        new BigDecimal("25.00"));

    assertThrows(
        IdempotencyConflictException.class,
        () -> accept("request-003", new BigDecimal("30.00")));

    assertEquals(1, countTransfers());
    assertEquals(1, countIdempotencyRecords());
    assertEquals(1, countOutboxEvents());

    UUID persistedTransferId = jdbcTemplate.queryForObject("""
        SELECT transfer_id
        FROM idempotency_records
        WHERE client_id = ?
          AND idempotency_key = ?
        """,
        UUID.class,
        clientId,
        "request-003");

    assertEquals(first.transferId(), persistedTransferId);
  }

  @Test
  void shouldRollbackAllWritesWhenOutboxInsertFails() {
    doThrow(new IllegalStateException("Simulated outbox failure"))
        .when(outboxRepository)
        .insertTransferAccepted(any(UUID.class), any(UUID.class));

    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> accept("request-004", new BigDecimal("40.00")));

    assertEquals("Simulated outbox failure", exception.getMessage());

    verify(outboxRepository)
        .insertTransferAccepted(any(UUID.class), any(UUID.class));

    assertEquals(0, countTransfers());
    assertEquals(0, countIdempotencyRecords());
    assertEquals(0, countOutboxEvents());
  }

  @Test
  void shouldCreateOnlyOneTransferForConcurrentIdenticalRequests()
      throws Exception {

    String idempotencyKey = "concurrent-request-001";
    BigDecimal amount = new BigDecimal("25.00");

    int numberOfRequests = 2;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfRequests);
    CountDownLatch ready = new CountDownLatch(numberOfRequests);
    CountDownLatch start = new CountDownLatch(1);

    try {
      Callable<TransferAcceptanceResult> request = () -> {
        ready.countDown();

        if (!start.await(10, TimeUnit.SECONDS)) {
          throw new TimeoutException("Timed out waiting to start");
        }

        return accept(idempotencyKey, amount);
      };

      Future<TransferAcceptanceResult> first = executor.submit(request);

      Future<TransferAcceptanceResult> second = executor.submit(request);

      assertTrue(ready.await(10, TimeUnit.SECONDS));

      // Libera as duas requisições para competir pela mesma chave.
      start.countDown();

      TransferAcceptanceResult resultA = first.get(20, TimeUnit.SECONDS);

      TransferAcceptanceResult resultB = second.get(20, TimeUnit.SECONDS);

      assertEquals(resultA.transferId(), resultB.transferId());

      assertNotEquals(
          resultA.replayed(),
          resultB.replayed(),
          "Uma resposta deve ser nova e a outra deve ser uma repetição");

      assertEquals(1, countTransfers());
      assertEquals(1, countIdempotencyRecords());
      assertEquals(1, countOutboxEvents());

    } finally {
      start.countDown();
      executor.shutdownNow();

      assertTrue(
          executor.awaitTermination(10, TimeUnit.SECONDS),
          "Executor did not terminate");
    }
  }

  @Test
  void shouldRejectConcurrentRequestsWithSameKeyAndDifferentAmounts()
      throws Exception {

    String idempotencyKey = "concurrent-conflict-001";

    int numberOfRequests = 2;
    ExecutorService executor = Executors.newFixedThreadPool(numberOfRequests);
    CountDownLatch ready = new CountDownLatch(numberOfRequests);
    CountDownLatch start = new CountDownLatch(1);

    try {
      Callable<TransferAcceptanceResult> request25 = () -> {
        ready.countDown();

        if (!start.await(10, TimeUnit.SECONDS)) {
          throw new TimeoutException("Timed out waiting to start");
        }

        return accept(
            idempotencyKey,
            new BigDecimal("25.00"));
      };

      Callable<TransferAcceptanceResult> request30 = () -> {
        ready.countDown();

        if (!start.await(10, TimeUnit.SECONDS)) {
          throw new TimeoutException("Timed out waiting to start");
        }

        return accept(
            idempotencyKey,
            new BigDecimal("30.00"));
      };

      Future<TransferAcceptanceResult> first = executor.submit(request25);

      Future<TransferAcceptanceResult> second = executor.submit(request30);

      assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();

      TransferAcceptanceResult winner = null;
      int conflicts = 0;

      for (Future<TransferAcceptanceResult> future : List.of(first, second)) {

        try {
          TransferAcceptanceResult result = future.get(20, TimeUnit.SECONDS);

          assertNull(
              winner,
              "Only one request should be accepted");

          winner = result;

        } catch (ExecutionException exception) {
          assertInstanceOf(
              IdempotencyConflictException.class,
              exception.getCause());
          conflicts++;
        }
      }

      assertNotNull(winner);
      assertFalse(winner.replayed());
      assertEquals(1, conflicts);

      // Apenas a requisição vencedora deve permanecer persistida.
      assertEquals(1, countTransfers());
      assertEquals(1, countIdempotencyRecords());
      assertEquals(1, countOutboxEvents());

      BigDecimal persistedAmount = jdbcTemplate.queryForObject("""
          SELECT amount
          FROM transfer_requests
          WHERE id = ?
          """,
          BigDecimal.class,
          winner.transferId());

      assertTrue(
          persistedAmount.compareTo(new BigDecimal("25.00")) == 0
              || persistedAmount.compareTo(new BigDecimal("30.00")) == 0);

    } finally {
      start.countDown();
      executor.shutdownNow();

      assertTrue(
          executor.awaitTermination(10, TimeUnit.SECONDS),
          "Executor did not terminate");
    }
  }

  private TransferAcceptanceResult accept(
      String idempotencyKey,
      BigDecimal amount) {
    return transferService.acceptTransfer(
        clientId,
        idempotencyKey,
        fromAccountId,
        toAccountId,
        amount,
        EUR);
  }

  private void insertAccount(UUID accountId, BigDecimal balance) {
    jdbcTemplate.update("""
        INSERT INTO accounts (
            id,
            currency,
            balance,
            status,
            created_at,
            updated_at
        )
        VALUES (?, 'EUR', ?, 'ACTIVE', CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP)
        """,
        accountId,
        balance);
  }

  private int countTransfers() {
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM transfer_requests",
        Integer.class);
  }

  private int countIdempotencyRecords() {
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM idempotency_records",
        Integer.class);
  }

  private int countOutboxEvents() {
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox_events",
        Integer.class);
  }
}
