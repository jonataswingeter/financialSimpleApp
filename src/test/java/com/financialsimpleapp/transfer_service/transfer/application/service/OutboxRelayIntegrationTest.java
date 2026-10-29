package com.financialsimpleapp.transfer_service.transfer.application.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import com.financialsimpleapp.transfer_service.TestcontainersConfiguration;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.RelayOutboxEventsUseCase;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.ClaimedOutboxEvent;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.EventPublishingException;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.OutboxRepository;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferEventPublisher;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferAcceptedEvent;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Verifies the outbox claim/lease protocol against real PostgreSQL: leases keep an event with one
 * relay instance, an expired lease hands it to another, a stale instance cannot overwrite the new
 * owner's outcome, concurrent relays never publish the same event twice, and failed sends back off
 * exponentially via next_attempt_at.
 *
 * <p>The broker is replaced by a mock that counts sends per transfer; the scheduled relay is
 * disabled (test default), so every relay run here is explicit.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OutboxRelayIntegrationTest {

  private static final Duration LEASE = Duration.ofMinutes(1);
  private static final int WORKERS = 4;

  // Far more rounds than draining legitimately needs (100 events / 7 per batch).
  private static final int MAX_ROUNDS = 50;

  @Autowired private OutboxRepository outboxRepository;

  @Autowired private RelayOutboxEventsUseCase relayOutboxEvents;

  @Autowired private JdbcTemplate jdbcTemplate;

  @MockitoBean private TransferEventPublisher eventPublisher;

  private final Map<UUID, AtomicInteger> sendsPerTransfer = new ConcurrentHashMap<>();

  @BeforeEach
  void setUp() {
    jdbcTemplate.update("DELETE FROM outbox_events");

    // Every send succeeds; record it so tests can count deliveries per transfer.
    doAnswer(
            invocation -> {
              UUID transferId = invocation.getArgument(0);
              sendsPerTransfer
                  .computeIfAbsent(transferId, id -> new AtomicInteger())
                  .incrementAndGet();
              return null;
            })
        .when(eventPublisher)
        .publish(any(UUID.class), anyString());
  }

  // ---------------------------------------------------------------------------------------------
  // #4 Lease expiry
  // ---------------------------------------------------------------------------------------------

  @Test
  void shouldNotHandOutAnEventWhileItsLeaseIsActive() {
    TransferAcceptedEvent event = insertEvent();

    List<ClaimedOutboxEvent> first = outboxRepository.claimPending(10, LEASE);
    List<ClaimedOutboxEvent> second = outboxRepository.claimPending(10, LEASE);

    assertEquals(List.of(event.eventId()), eventIds(first));
    assertTrue(second.isEmpty(), "A leased event must not be claimed again");

    // Another relay instance finds nothing to do either.
    assertEquals(0, relayOutboxEvents.relayPendingEvents());
    assertTrue(sendsPerTransfer.isEmpty());
  }

  /**
   * The instance sent the event and Kafka confirmed it, but the instance died before marking it
   * published. Once the lease expires, another instance re-sends it: delivery is at-least-once, and
   * the consumer's idempotency (see the end-to-end duplicate-delivery test) absorbs the copy.
   */
  @Test
  void shouldRepublishEventWhenOwnerCrashedAfterSendingButBeforeMarking() {
    TransferAcceptedEvent event = insertEvent();

    ClaimedOutboxEvent crashed = single(outboxRepository.claimPending(10, LEASE));
    eventPublisher.publish(crashed.transferId(), crashed.payload());
    // ...crash: markPublished is never called.

    expireLease(event.eventId());

    assertEquals(1, relayOutboxEvents.relayPendingEvents());
    assertEquals(2, sendsPerTransfer.get(event.transferId()).get());
    assertPublishedAndReleased(event.eventId());
  }

  @Test
  void shouldIgnoreOutcomeReportedByInstanceThatLostItsLease() {
    TransferAcceptedEvent event = insertEvent();

    ClaimedOutboxEvent stale = single(outboxRepository.claimPending(10, LEASE));
    expireLease(event.eventId());
    ClaimedOutboxEvent current = single(outboxRepository.claimPending(10, LEASE));

    assertEquals(stale.eventId(), current.eventId());
    assertNotEquals(stale.lockToken(), current.lockToken(), "A new claim gets a new lock token");

    // The previous owner wakes up and tries to report an outcome: both are refused.
    assertFalse(outboxRepository.markPublished(event.eventId(), stale.lockToken()));
    assertFalse(
        outboxRepository.scheduleRetry(event.eventId(), stale.lockToken(), Duration.ofSeconds(1)));

    Map<String, Object> row = outboxRow(event.eventId());
    assertNull(row.get("published_at"));
    assertEquals(0, row.get("attempts"));
    assertEquals(current.lockToken(), row.get("lock_token"));

    // The current owner is still in control.
    assertTrue(outboxRepository.markPublished(event.eventId(), current.lockToken()));
    assertPublishedAndReleased(event.eventId());
  }

  // ---------------------------------------------------------------------------------------------
  // #5 Concurrent relays
  // ---------------------------------------------------------------------------------------------

  @Test
  void shouldGiveEachEventToExactlyOneOfConcurrentClaimers() throws Exception {
    Set<UUID> inserted = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      inserted.add(insertEvent().eventId());
    }

    Queue<UUID> claimed = new ConcurrentLinkedQueue<>();

    runConcurrently(
        () -> {
          // Keep claiming small batches until nothing is left for this worker. Bounded, so a
          // broken lease (events claimable forever) fails the assertions instead of hanging.
          for (int round = 0; round < MAX_ROUNDS; round++) {
            List<ClaimedOutboxEvent> batch = outboxRepository.claimPending(7, LEASE);
            if (batch.isEmpty()) {
              break;
            }
            claimed.addAll(eventIds(batch));
          }
          return null;
        });

    assertEquals(inserted.size(), claimed.size(), "No event may be claimed twice");
    assertEquals(inserted, new HashSet<>(claimed), "Every event must be claimed");
  }

  @Test
  void shouldPublishEachEventExactlyOnceWithConcurrentRelays() throws Exception {
    Set<UUID> transferIds = new HashSet<>();
    for (int i = 0; i < 60; i++) {
      transferIds.add(insertEvent().transferId());
    }

    runConcurrently(
        () -> {
          // Drain until this relay instance finds nothing more to claim.
          for (int round = 0; round < MAX_ROUNDS; round++) {
            if (relayOutboxEvents.relayPendingEvents() == 0) {
              break;
            }
          }
          return null;
        });

    assertEquals(transferIds, sendsPerTransfer.keySet(), "Every event must be sent");
    sendsPerTransfer.forEach(
        (transferId, sends) ->
            assertEquals(1, sends.get(), "Transfer " + transferId + " was sent more than once"));

    assertEquals(
        0,
        jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*)
            FROM outbox_events
            WHERE published_at IS NULL OR lock_token IS NOT NULL
            """,
            Integer.class),
        "Every event must be marked published with its lease released");
  }

  // ---------------------------------------------------------------------------------------------
  // #8 Retry backoff stored in the database
  // ---------------------------------------------------------------------------------------------

  /**
   * Every failed send stores the next attempt time in the row itself, so the backoff survives
   * restarts and applies to every relay instance. Delays follow the configured defaults: retry-base
   * 1s, doubling per attempt, capped at retry-max 5m.
   */
  @Test
  void shouldBackOffExponentiallyBetweenFailedSendsAndPublishOnceTheBrokerRecovers() {
    doThrow(new EventPublishingException("Simulated broker outage", null))
        .when(eventPublisher)
        .publish(any(UUID.class), anyString());

    TransferAcceptedEvent event = insertEvent();

    assertRetryScheduledAfterFailedRelay(event.eventId(), 1, Duration.ofSeconds(1));

    // Backing off: no relay instance may pick the event up before its next attempt time.
    assertTrue(outboxRepository.claimPending(10, LEASE).isEmpty());
    assertEquals(0, relayOutboxEvents.relayPendingEvents());

    makeDue(event.eventId());
    assertRetryScheduledAfterFailedRelay(event.eventId(), 2, Duration.ofSeconds(2));

    makeDue(event.eventId());
    assertRetryScheduledAfterFailedRelay(event.eventId(), 3, Duration.ofSeconds(4));

    // The broker is back.
    doNothing().when(eventPublisher).publish(any(UUID.class), anyString());
    makeDue(event.eventId());

    assertEquals(1, relayOutboxEvents.relayPendingEvents());
    assertPublishedAndReleased(event.eventId());
    assertEquals(3, outboxRow(event.eventId()).get("attempts"), "A success is not an attempt");
  }

  @Test
  void shouldCapTheBackoffAtTheConfiguredMaximum() {
    doThrow(new EventPublishingException("Simulated broker outage", null))
        .when(eventPublisher)
        .publish(any(UUID.class), anyString());

    TransferAcceptedEvent event = insertEvent();
    // A long outage: 2^30 seconds would be decades.
    jdbcTemplate.update(
        "UPDATE outbox_events SET attempts = 30 WHERE event_id = ?", event.eventId());

    assertRetryScheduledAfterFailedRelay(event.eventId(), 31, Duration.ofMinutes(5));
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * Runs one relay that fails to send, and checks the stored retry: attempts incremented, lease
   * released, and next_attempt_at exactly {@code delay} after the failure (bracketed by the
   * database clock before and after the relay).
   */
  private void assertRetryScheduledAfterFailedRelay(
      UUID eventId, int expectedAttempts, Duration delay) {
    Instant before = databaseNow();
    assertEquals(0, relayOutboxEvents.relayPendingEvents());
    Instant after = databaseNow();

    Map<String, Object> row = outboxRow(eventId);
    assertNull(row.get("published_at"));
    assertNull(row.get("lock_token"), "A failed send must release the lease");
    assertEquals(expectedAttempts, row.get("attempts"));

    Instant nextAttempt = ((Timestamp) row.get("next_attempt_at")).toInstant();
    assertFalse(
        nextAttempt.isBefore(before.plus(delay)) || nextAttempt.isAfter(after.plus(delay)),
        "Expected next attempt "
            + delay
            + " after the failure, between "
            + before.plus(delay)
            + " and "
            + after.plus(delay)
            + ", but was "
            + nextAttempt);
  }

  /** Simulates the backoff delay passing, without sleeping. */
  private void makeDue(UUID eventId) {
    jdbcTemplate.update(
        """
        UPDATE outbox_events
        SET next_attempt_at = clock_timestamp() - interval '1 second'
        WHERE event_id = ?
        """,
        eventId);
  }

  private Instant databaseNow() {
    return jdbcTemplate.queryForObject("SELECT clock_timestamp()", Timestamp.class).toInstant();
  }

  private TransferAcceptedEvent insertEvent() {
    TransferAcceptedEvent event = new TransferAcceptedEvent(UUID.randomUUID(), UUID.randomUUID());
    outboxRepository.insertTransferAccepted(event);
    return event;
  }

  /** Simulates time passing beyond the lease, without sleeping. */
  private void expireLease(UUID eventId) {
    int updated =
        jdbcTemplate.update(
            """
            UPDATE outbox_events
            SET locked_until = clock_timestamp() - interval '1 second'
            WHERE event_id = ? AND lock_token IS NOT NULL
            """,
            eventId);

    assertEquals(1, updated, "Event " + eventId + " was not leased");
  }

  private Map<String, Object> outboxRow(UUID eventId) {
    return jdbcTemplate.queryForMap(
        """
        SELECT published_at, attempts, next_attempt_at, locked_until, lock_token
        FROM outbox_events
        WHERE event_id = ?
        """,
        eventId);
  }

  private void assertPublishedAndReleased(UUID eventId) {
    Map<String, Object> row = outboxRow(eventId);

    assertNotNull(row.get("published_at"));
    assertNull(row.get("locked_until"));
    assertNull(row.get("lock_token"));
  }

  private static List<UUID> eventIds(List<ClaimedOutboxEvent> events) {
    return events.stream().map(ClaimedOutboxEvent::eventId).toList();
  }

  private static ClaimedOutboxEvent single(List<ClaimedOutboxEvent> events) {
    assertEquals(1, events.size(), "Expected exactly one claimed event");
    return events.getFirst();
  }

  /** Runs the task on {@link #WORKERS} threads that all start at the same moment. */
  private static void runConcurrently(Callable<Void> task) throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Void>> results = new ArrayList<>();

    ExecutorService executor = Executors.newFixedThreadPool(WORKERS);
    try {
      for (int i = 0; i < WORKERS; i++) {
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  return task.call();
                }));
      }

      start.countDown();

      for (Future<Void> result : results) {
        result.get(30, TimeUnit.SECONDS);
      }
    } finally {
      // Interrupt stragglers instead of waiting for them, so a failure never hangs the build.
      executor.shutdownNow();
    }
  }
}
