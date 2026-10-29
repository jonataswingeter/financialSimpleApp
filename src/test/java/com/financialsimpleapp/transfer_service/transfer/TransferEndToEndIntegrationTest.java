package com.financialsimpleapp.transfer_service.transfer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.financialsimpleapp.transfer_service.TestcontainersConfiguration;
import com.financialsimpleapp.transfer_service.transfer.adapter.in.web.TransferAcceptanceResponse;
import com.financialsimpleapp.transfer_service.transfer.adapter.in.web.TransferRequest;
import com.financialsimpleapp.transfer_service.transfer.adapter.in.web.TransferStatusResponse;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.ProcessTransferUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.EventPublishingException;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferEventPublisher;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferProcessingRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.LedgerEntry;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferAcceptedEvent;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
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
      // Short backoff so the broker-outage scenario recovers quickly.
      "app.outbox.retry-base=PT0.1S",
      "app.outbox.retry-max=PT1S",
      // The listener may join the group after the event was published.
      "spring.kafka.consumer.auto-offset-reset=earliest"
    })
@AutoConfigureRestTestClient
@Import(TestcontainersConfiguration.class)
class TransferEndToEndIntegrationTest {

  private static final Duration PROCESSING_TIMEOUT = Duration.ofSeconds(30);

  @Autowired private RestTestClient restTestClient;

  @Autowired private JdbcTemplate jdbcTemplate;

  @Autowired private KafkaTemplate<String, String> kafkaTemplate;

  @Value("${app.kafka.topics.transfer-accepted:transfer.accepted}")
  private String topic;

  // Spies call the real beans; they let a test count calls or inject failures.
  @MockitoSpyBean private TransferEventPublisher eventPublisher;

  @MockitoSpyBean private ProcessTransferUseCase processTransferUseCase;

  @MockitoSpyBean private TransferProcessingRepository processingRepository;

  @Autowired private ConsumerFactory<String, String> consumerFactory;

  @Autowired private ObjectMapper objectMapper;

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

  /**
   * Delivery is at-least-once: the relay re-sends an event whose lease expired after Kafka had
   * already confirmed it, and Kafka itself may redeliver. Money must still move exactly once.
   */
  @Test
  void shouldMoveMoneyOnlyOnceWhenTheSameEventIsDeliveredRepeatedly() throws Exception {
    // A duplicate must be a silent no-op. Throwing instead would also keep the money safe, but
    // every duplicate would then be retried and end up in the dead-letter topic.
    List<Throwable> processingFailures = new CopyOnWriteArrayList<>();
    doAnswer(
            invocation -> {
              try {
                return invocation.callRealMethod();
              } catch (Throwable failure) {
                processingFailures.add(failure);
                throw failure;
              }
            })
        .when(processTransferUseCase)
        .process(any(UUID.class));

    UUID transferId = acceptViaApi("duplicate-delivery-001", "25.00");
    awaitStatus(transferId, TransferStatus.COMPLETED);

    String payload =
        jdbcTemplate.queryForObject(
            "SELECT payload::text FROM outbox_events WHERE aggregate_id = ?",
            String.class,
            transferId);

    // Redeliver the exact same event twice.
    for (int i = 0; i < 2; i++) {
      kafkaTemplate.send(topic, transferId.toString(), payload).get(10, TimeUnit.SECONDS);
    }

    // All three deliveries reach the use case, and none of them fails.
    verify(processTransferUseCase, timeout(PROCESSING_TIMEOUT.toMillis()).atLeast(3))
        .process(transferId);
    assertTrue(processingFailures.isEmpty(), "Duplicate delivery failed: " + processingFailures);
    verify(processTransferUseCase, times(3)).process(transferId);

    assertEquals(TransferStatus.COMPLETED, getTransfer(transferId).status());
    assertBalance(fromAccountId, "975.00");
    assertBalance(toAccountId, "525.00");
    assertEquals(2, countLedgerEntries(transferId));
  }

  /**
   * The outbox guarantees no event is lost while the broker is down: the send fails, a retry is
   * scheduled, and the event is published (and processed) once the broker is back.
   */
  @Test
  void shouldRetryAndEventuallyPublishWhenBrokerIsTemporarilyUnavailable() {
    EventPublishingException brokerDown =
        new EventPublishingException("Simulated broker outage", null);

    doThrow(brokerDown)
        .doThrow(brokerDown)
        .doCallRealMethod()
        .when(eventPublisher)
        .publish(any(UUID.class), anyString());

    UUID transferId = acceptViaApi("broker-outage-001", "25.00");
    awaitStatus(transferId, TransferStatus.COMPLETED);

    verify(eventPublisher, times(3)).publish(eq(transferId), anyString());

    Map<String, Object> outboxEvent =
        jdbcTemplate.queryForMap(
            """
            SELECT attempts, published_at, locked_until, lock_token
            FROM outbox_events
            WHERE aggregate_id = ?
            """,
            transferId);

    assertEquals(2, outboxEvent.get("attempts"), "One attempt recorded per failed send");
    assertNotNull(outboxEvent.get("published_at"));
    assertNull(outboxEvent.get("locked_until"));
    assertNull(outboxEvent.get("lock_token"));

    assertBalance(fromAccountId, "975.00");
    assertBalance(toAccountId, "525.00");
    assertEquals(2, countLedgerEntries(transferId));
  }

  /**
   * A business rule violation is a normal outcome, not a failure: the event is still published and
   * consumed, and the transfer ends REJECTED with its reason and no money moved.
   */
  @Test
  void shouldRejectTransferWithoutMovingMoneyWhenFundsAreInsufficient() {
    UUID transferId = acceptViaApi("insufficient-funds-001", "1000.01");

    TransferStatusResponse rejected = awaitStatus(transferId, TransferStatus.REJECTED);

    assertEquals("INSUFFICIENT_FUNDS", rejected.rejectionCode());
    assertNull(rejected.completedAt());
    assertNothingMoved(transferId);
    assertEventPublished(transferId);
  }

  /** Account rules are checked when the transfer is processed, not when it is accepted. */
  @Test
  void shouldRejectTransferWhenSourceAccountWasBlockedAfterAcceptance() {
    // Hold the event in the outbox (the broker is "down") until the account is blocked.
    AtomicBoolean accountBlocked = new AtomicBoolean();
    doAnswer(
            invocation -> {
              if (!accountBlocked.get()) {
                throw new EventPublishingException("Simulated broker outage", null);
              }
              return invocation.callRealMethod();
            })
        .when(eventPublisher)
        .publish(any(UUID.class), anyString());

    UUID transferId = acceptViaApi("blocked-source-001", "25.00");
    jdbcTemplate.update("UPDATE accounts SET status = 'BLOCKED' WHERE id = ?", fromAccountId);
    accountBlocked.set(true);

    TransferStatusResponse rejected = awaitStatus(transferId, TransferStatus.REJECTED);

    assertEquals("SOURCE_ACCOUNT_NOT_ACTIVE", rejected.rejectionCode());
    assertNothingMoved(transferId);
  }

  /**
   * Processing fails halfway: the source is debited, its balance saved and the debit ledger entry
   * written, then writing the credit fails. The whole transaction must roll back, so Kafka's retry
   * starts from a clean state and moves the money exactly once. Without the rollback the retry
   * would debit twice and hit the unique (transfer_id, entry_type) ledger constraint forever.
   */
  @Test
  void shouldRollBackPartialProcessingAndCompleteOnKafkaRetry() {
    doCallRealMethod()
        .doThrow(new IllegalStateException("Simulated database failure"))
        .doCallRealMethod()
        .when(processingRepository)
        .insertLedgerEntry(any(LedgerEntry.class));

    UUID transferId = acceptViaApi("partial-failure-001", "25.00");
    awaitStatus(transferId, TransferStatus.COMPLETED);

    // The first delivery failed, the redelivery succeeded.
    verify(processTransferUseCase, times(2)).process(transferId);
    verify(processingRepository, times(4)).insertLedgerEntry(any(LedgerEntry.class));

    assertBalance(fromAccountId, "975.00");
    assertBalance(toAccountId, "525.00");
    assertEquals(2, countLedgerEntries(transferId));
  }

  /**
   * A message that can never be processed (here: not even JSON) must not block its partition. After
   * the retries it is parked in the dead-letter topic with the failure attached, and later
   * transfers on the same partitions are still processed.
   */
  @Test
  void shouldParkUnreadableMessageInDeadLetterTopicAndKeepConsuming() throws Exception {
    String key = "poison-" + UUID.randomUUID();
    int partitions = kafkaTemplate.partitionsFor(topic).size();

    // One poison message in front of every partition, so the next transfer queues behind one.
    for (int partition = 0; partition < partitions; partition++) {
      kafkaTemplate.send(topic, partition, key, "{not json").get(10, TimeUnit.SECONDS);
    }

    UUID transferId = acceptViaApi("after-poison-001", "25.00");
    awaitStatus(transferId, TransferStatus.COMPLETED);

    List<ConsumerRecord<String, String>> parked = awaitDeadLetters(key, partitions);

    for (ConsumerRecord<String, String> record : parked) {
      assertEquals("{not json", record.value());
      assertEquals(topic, header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC));
      assertEquals(
          JsonParseException.class.getName(),
          header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN));
    }

    assertBalance(fromAccountId, "975.00");
    assertBalance(toAccountId, "525.00");
  }

  /**
   * A well-formed event for a transfer that does not exist fails on every attempt; it ends up in
   * the dead-letter topic instead of being retried forever.
   */
  @Test
  void shouldParkEventForUnknownTransferInDeadLetterTopic() throws Exception {
    UUID unknownTransferId = UUID.randomUUID();
    String payload =
        objectMapper.writeValueAsString(
            new TransferAcceptedEvent(UUID.randomUUID(), unknownTransferId));

    kafkaTemplate.send(topic, unknownTransferId.toString(), payload).get(10, TimeUnit.SECONDS);

    ConsumerRecord<String, String> parked =
        awaitDeadLetters(unknownTransferId.toString(), 1).getFirst();

    assertEquals(payload, parked.value());
    assertEquals(
        IllegalStateException.class.getName(),
        header(parked, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN));
    assertTrue(
        header(parked, KafkaHeaders.DLT_EXCEPTION_MESSAGE).contains(unknownTransferId.toString()));

    // Initial delivery plus the error handler's 3 retries.
    verify(processTransferUseCase, times(4)).process(unknownTransferId);
  }

  /** Reads the dead-letter topic from the beginning until {@code expected} records with the key. */
  private List<ConsumerRecord<String, String>> awaitDeadLetters(String key, int expected) {
    List<ConsumerRecord<String, String>> found = new ArrayList<>();

    try (Consumer<String, String> consumer =
        consumerFactory.createConsumer("dlt-reader-" + UUID.randomUUID(), null)) {
      consumer.subscribe(List.of(topic + ".DLT"));

      await()
          .atMost(PROCESSING_TIMEOUT)
          .until(
              () -> {
                consumer
                    .poll(Duration.ofMillis(500))
                    .forEach(
                        record -> {
                          if (key.equals(record.key())) {
                            found.add(record);
                          }
                        });
                return found.size() >= expected;
              });
    }

    assertEquals(expected, found.size(), "Unexpected dead letters for key " + key);
    return found;
  }

  private static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    assertNotNull(header, "Missing header " + name);
    return new String(header.value(), StandardCharsets.UTF_8);
  }

  private void assertNothingMoved(UUID transferId) {
    assertBalance(fromAccountId, "1000.00");
    assertBalance(toAccountId, "500.00");
    assertEquals(0, countLedgerEntries(transferId));
  }

  private void assertEventPublished(UUID transferId) {
    assertNotNull(
        jdbcTemplate.queryForObject(
            "SELECT published_at FROM outbox_events WHERE aggregate_id = ?",
            Timestamp.class,
            transferId));
  }

  private UUID acceptViaApi(String idempotencyKey, String amount) {
    TransferAcceptanceResponse accepted =
        restTestClient
            .post()
            .uri("/api/v1/transfers")
            .header("X-Client-Id", clientId.toString())
            .header("Idempotency-Key", idempotencyKey)
            .body(new TransferRequest(fromAccountId, toAccountId, new BigDecimal(amount), "EUR"))
            .exchange()
            .expectStatus()
            .isEqualTo(HttpStatus.ACCEPTED)
            .expectBody(TransferAcceptanceResponse.class)
            .returnResult()
            .getResponseBody();

    assertNotNull(accepted);
    return accepted.transferId();
  }

  private TransferStatusResponse awaitStatus(UUID transferId, TransferStatus expected) {
    return await()
        .atMost(PROCESSING_TIMEOUT)
        .pollInterval(Duration.ofMillis(200))
        .until(() -> getTransfer(transferId), status -> status.status() == expected);
  }

  private int countLedgerEntries(UUID transferId) {
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM ledger_entries WHERE transfer_id = ?", Integer.class, transferId);
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
