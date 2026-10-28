package com.financialsimpleapp.transfer_service.transfer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import com.financialsimpleapp.transfer_service.TestcontainersConfiguration;
import com.financialsimpleapp.transfer_service.transfer.adapter.in.web.TransferAcceptanceResponse;
import com.financialsimpleapp.transfer_service.transfer.adapter.in.web.TransferRequest;
import com.financialsimpleapp.transfer_service.transfer.adapter.in.web.TransferStatusResponse;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * Exercises the whole transactional outbox flow against real PostgreSQL and Kafka containers: HTTP
 * request -> transfer + outbox row (same transaction) -> OutboxRelayService -> Kafka ->
 * TransferAcceptedListener -> balances, ledger entries and COMPLETED status.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "app.outbox.scheduling.enabled=true",
      "app.outbox.poll-delay-ms=200",
      // The listener may join the group after the event was published.
      "spring.kafka.consumer.auto-offset-reset=earliest"
    })
@AutoConfigureRestTestClient
@Import(TestcontainersConfiguration.class)
class TransferEndToEndIntegrationTest {

  private static final Duration PROCESSING_TIMEOUT = Duration.ofSeconds(30);

  @Autowired private RestTestClient restTestClient;

  @Autowired private JdbcTemplate jdbcTemplate;

  private final UUID clientId = UUID.randomUUID();
  private final UUID fromAccountId = UUID.randomUUID();
  private final UUID toAccountId = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    jdbcTemplate.update("DELETE FROM outbox_events");
    jdbcTemplate.update("DELETE FROM ledger_entries");
    jdbcTemplate.update("DELETE FROM idempotency_records");
    jdbcTemplate.update("DELETE FROM transfer_requests");
    jdbcTemplate.update("DELETE FROM accounts");

    insertAccount(fromAccountId, new BigDecimal("1000.00"));
    insertAccount(toAccountId, new BigDecimal("500.00"));
  }

  @Test
  void shouldAcceptTransferViaApiAndCompleteItAsynchronously() {
    // 1. The API accepts the request and answers 202 immediately.
    TransferAcceptanceResponse accepted =
        restTestClient
            .post()
            .uri("/api/v1/transfers")
            .header("X-Client-Id", clientId.toString())
            .header("Idempotency-Key", "happy-path-001")
            .body(new TransferRequest(fromAccountId, toAccountId, new BigDecimal("25.00"), "EUR"))
            .exchange()
            .expectStatus()
            .isEqualTo(HttpStatus.ACCEPTED)
            .expectBody(TransferAcceptanceResponse.class)
            .returnResult()
            .getResponseBody();

    assertNotNull(accepted);
    assertNotNull(accepted.transferId());
    assertEquals(TransferStatus.ACCEPTED, accepted.status());
    assertFalse(accepted.replayed());

    UUID transferId = accepted.transferId();

    // 2. The transfer is processed asynchronously through outbox + Kafka.
    TransferStatusResponse completed =
        await()
            .atMost(PROCESSING_TIMEOUT)
            .pollInterval(Duration.ofMillis(200))
            .until(
                () -> getTransfer(transferId),
                status -> status.status() == TransferStatus.COMPLETED);

    assertEquals(transferId, completed.transferId());
    assertNull(completed.rejectionCode());
    assertNotNull(completed.createdAt());
    assertNotNull(completed.completedAt());

    // 3. Money moved exactly once.
    assertBalance(fromAccountId, "975.00");
    assertBalance(toAccountId, "525.00");

    List<Map<String, Object>> ledger =
        jdbcTemplate.queryForList(
            """
            SELECT account_id, entry_type, amount, currency
            FROM ledger_entries
            WHERE transfer_id = ?
            ORDER BY entry_type
            """,
            transferId);

    assertEquals(2, ledger.size());

    assertEquals(toAccountId, ledger.get(0).get("account_id"));
    assertEquals("CREDIT", ledger.get(0).get("entry_type"));
    assertEquals(0, new BigDecimal("25.00").compareTo((BigDecimal) ledger.get(0).get("amount")));
    assertEquals("EUR", ledger.get(0).get("currency"));

    assertEquals(fromAccountId, ledger.get(1).get("account_id"));
    assertEquals("DEBIT", ledger.get(1).get("entry_type"));
    assertEquals(0, new BigDecimal("-25.00").compareTo((BigDecimal) ledger.get(1).get("amount")));
    assertEquals("EUR", ledger.get(1).get("currency"));

    // 4. The outbox event was published and its lease released.
    Map<String, Object> outboxEvent =
        jdbcTemplate.queryForMap(
            """
            SELECT event_type, published_at, locked_until, lock_token
            FROM outbox_events
            WHERE aggregate_id = ?
            """,
            transferId);

    assertEquals("TRANSFER_ACCEPTED", outboxEvent.get("event_type"));
    assertNotNull(outboxEvent.get("published_at"));
    assertNull(outboxEvent.get("locked_until"));
    assertNull(outboxEvent.get("lock_token"));
  }

  private TransferStatusResponse getTransfer(UUID transferId) {
    return restTestClient
        .get()
        .uri(URI.create("/api/v1/transfers/" + transferId))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(TransferStatusResponse.class)
        .returnResult()
        .getResponseBody();
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
}
