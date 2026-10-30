package com.financialsimpleapp.transfer_service.transfer.adapter.out.outbox;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxRelayTest {

  private static final OutboxRelaySettings SETTINGS =
      new OutboxRelaySettings(
          10,
          Duration.ofMinutes(5),
          Duration.ofSeconds(1),
          Duration.ofSeconds(1),
          Duration.ofSeconds(10));

  private JdbcOutboxRelayStore relayStore;
  private KafkaOutboxPublisher eventPublisher;
  private OutboxRelay relay;

  @BeforeEach
  void setUp() {
    relayStore = mock(JdbcOutboxRelayStore.class);
    eventPublisher = mock(KafkaOutboxPublisher.class);
    relay = new OutboxRelay(relayStore, eventPublisher, SETTINGS);
  }

  @Test
  void shouldPublishClaimedEventsAndMarkThemPublished() {
    ClaimedOutboxEvent first = event(0);
    ClaimedOutboxEvent second = event(0);
    givenClaimed(first, second);
    when(relayStore.markPublished(any(), any())).thenReturn(true);

    int published = relay.relayPendingEvents();

    assertEquals(2, published);
    verify(eventPublisher).publish(first.transferId(), first.payload());
    verify(eventPublisher).publish(second.transferId(), second.payload());
    verify(relayStore).markPublished(first.eventId(), first.lockToken());
    verify(relayStore).markPublished(second.eventId(), second.lockToken());
    verify(relayStore, never()).scheduleRetry(any(), any(), any());
  }

  @Test
  void shouldScheduleRetryWithBackoffWhenPublishingFails() {
    ClaimedOutboxEvent event = event(2);
    givenClaimed(event);
    doThrow(new EventPublishingException("broker down", null))
        .when(eventPublisher)
        .publish(any(), any());
    when(relayStore.scheduleRetry(any(), any(), any())).thenReturn(true);

    int published = relay.relayPendingEvents();

    assertEquals(0, published);
    verify(relayStore).scheduleRetry(event.eventId(), event.lockToken(), Duration.ofSeconds(4));
    verify(relayStore, never()).markPublished(any(), any());
  }

  @Test
  void shouldKeepRelayingRemainingEventsAfterOneFails() {
    ClaimedOutboxEvent failing = event(0);
    ClaimedOutboxEvent healthy = event(0);
    givenClaimed(failing, healthy);
    doThrow(new EventPublishingException("broker down", null))
        .when(eventPublisher)
        .publish(failing.transferId(), failing.payload());
    when(relayStore.markPublished(any(), any())).thenReturn(true);

    int published = relay.relayPendingEvents();

    assertEquals(1, published);
    verify(relayStore).markPublished(healthy.eventId(), healthy.lockToken());
  }

  @Test
  void shouldNotRetryWhenMarkingFailsAfterBrokerConfirmed() {
    ClaimedOutboxEvent event = event(0);
    givenClaimed(event);
    when(relayStore.markPublished(any(), any()))
        .thenThrow(new IllegalStateException("database down"));

    int published = relay.relayPendingEvents();

    assertEquals(1, published);
    verify(relayStore, never()).scheduleRetry(any(), any(), any());
  }

  @Test
  void shouldPublishNothingWhenClaimingFails() {
    when(relayStore.claimPending(anyInt(), any()))
        .thenThrow(new IllegalStateException("database down"));

    assertEquals(0, relay.relayPendingEvents());
    verifyNoInteractions(eventPublisher);
  }

  @Test
  void shouldClaimWithLeaseCoveringTheWholeBatch() {
    givenClaimed();

    relay.relayPendingEvents();

    verify(relayStore).claimPending(10, Duration.ofMinutes(5));
  }

  @Test
  void shouldGrowRetryDelayExponentiallyUpToMaximum() {
    assertEquals(Duration.ofSeconds(1), relay.retryDelay(0));
    assertEquals(Duration.ofSeconds(2), relay.retryDelay(1));
    assertEquals(Duration.ofSeconds(8), relay.retryDelay(3));
    assertEquals(Duration.ofSeconds(10), relay.retryDelay(4));
    assertEquals(Duration.ofSeconds(10), relay.retryDelay(1_000));
  }

  @Test
  void shouldExtendLeaseWhenConfiguredLockIsTooShort() {
    OutboxRelaySettings settings =
        new OutboxRelaySettings(
            100,
            Duration.ofSeconds(30),
            Duration.ofSeconds(10),
            Duration.ofSeconds(1),
            Duration.ofMinutes(5));

    // 100 events * 10s send timeout + 30s safety margin.
    assertEquals(Duration.ofSeconds(1_030), settings.leaseDuration());
  }

  private void givenClaimed(ClaimedOutboxEvent... events) {
    when(relayStore.claimPending(anyInt(), any())).thenReturn(List.of(events));
  }

  private static ClaimedOutboxEvent event(int attempts) {
    UUID eventId = UUID.randomUUID();
    UUID transferId = UUID.randomUUID();

    return new ClaimedOutboxEvent(
        eventId,
        transferId,
        "TRANSFER_ACCEPTED",
        """
        {"eventId":"%s","transferId":"%s"}\
        """
            .formatted(eventId, transferId),
        attempts,
        UUID.randomUUID());
  }
}
