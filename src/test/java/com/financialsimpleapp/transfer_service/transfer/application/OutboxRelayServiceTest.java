package com.financialsimpleapp.transfer_service.transfer.application;

import com.financialsimpleapp.transfer_service.transfer.application.port.out.ClaimedOutboxEvent;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.EventPublishingException;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.TransferEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

class OutboxRelayServiceTest {

  private static final OutboxRelaySettings SETTINGS = new OutboxRelaySettings(
      10,
      Duration.ofMinutes(5),
      Duration.ofSeconds(1),
      Duration.ofSeconds(1),
      Duration.ofSeconds(10));

  private OutboxTransactionService outboxTransactions;
  private TransferEventPublisher eventPublisher;
  private OutboxRelayService relay;

  @BeforeEach
  void setUp() {
    outboxTransactions = mock(OutboxTransactionService.class);
    eventPublisher = mock(TransferEventPublisher.class);
    relay = new OutboxRelayService(outboxTransactions, eventPublisher, SETTINGS);
  }

  @Test
  void shouldPublishClaimedEventsAndMarkThemPublished() {
    ClaimedOutboxEvent first = event(0);
    ClaimedOutboxEvent second = event(0);
    givenClaimed(first, second);
    when(outboxTransactions.markPublished(any(), any())).thenReturn(true);

    int published = relay.relayPendingEvents();

    assertEquals(2, published);
    verify(eventPublisher).publish(first.transferId(), first.payload());
    verify(eventPublisher).publish(second.transferId(), second.payload());
    verify(outboxTransactions).markPublished(first.eventId(), first.lockToken());
    verify(outboxTransactions).markPublished(second.eventId(), second.lockToken());
    verify(outboxTransactions, never()).scheduleRetry(any(), any(), any());
  }

  @Test
  void shouldScheduleRetryWithBackoffWhenPublishingFails() {
    ClaimedOutboxEvent event = event(2);
    givenClaimed(event);
    doThrow(new EventPublishingException("broker down", null))
        .when(eventPublisher).publish(any(), any());
    when(outboxTransactions.scheduleRetry(any(), any(), any())).thenReturn(true);

    int published = relay.relayPendingEvents();

    assertEquals(0, published);
    verify(outboxTransactions).scheduleRetry(
        event.eventId(), event.lockToken(), Duration.ofSeconds(4));
    verify(outboxTransactions, never()).markPublished(any(), any());
  }

  @Test
  void shouldKeepRelayingRemainingEventsAfterOneFails() {
    ClaimedOutboxEvent failing = event(0);
    ClaimedOutboxEvent healthy = event(0);
    givenClaimed(failing, healthy);
    doThrow(new EventPublishingException("broker down", null))
        .when(eventPublisher).publish(failing.transferId(), failing.payload());
    when(outboxTransactions.markPublished(any(), any())).thenReturn(true);

    int published = relay.relayPendingEvents();

    assertEquals(1, published);
    verify(outboxTransactions).markPublished(healthy.eventId(), healthy.lockToken());
  }

  @Test
  void shouldNotRetryWhenMarkingFailsAfterBrokerConfirmed() {
    ClaimedOutboxEvent event = event(0);
    givenClaimed(event);
    when(outboxTransactions.markPublished(any(), any()))
        .thenThrow(new IllegalStateException("database down"));

    int published = relay.relayPendingEvents();

    assertEquals(1, published);
    verify(outboxTransactions, never()).scheduleRetry(any(), any(), any());
  }

  @Test
  void shouldPublishNothingWhenClaimingFails() {
    when(outboxTransactions.claimPending(anyInt(), any()))
        .thenThrow(new IllegalStateException("database down"));

    assertEquals(0, relay.relayPendingEvents());
    verifyNoInteractions(eventPublisher);
  }

  @Test
  void shouldClaimWithLeaseCoveringTheWholeBatch() {
    givenClaimed();

    relay.relayPendingEvents();

    verify(outboxTransactions).claimPending(10, Duration.ofMinutes(5));
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
    OutboxRelaySettings settings = new OutboxRelaySettings(
        100,
        Duration.ofSeconds(30),
        Duration.ofSeconds(10),
        Duration.ofSeconds(1),
        Duration.ofMinutes(5));

    // 100 events * 10s send timeout + 30s safety margin.
    assertEquals(Duration.ofSeconds(1_030), settings.leaseDuration());
  }

  private void givenClaimed(ClaimedOutboxEvent... events) {
    when(outboxTransactions.claimPending(anyInt(), any()))
        .thenReturn(List.of(events));
  }

  private static ClaimedOutboxEvent event(int attempts) {
    UUID eventId = UUID.randomUUID();
    UUID transferId = UUID.randomUUID();

    return new ClaimedOutboxEvent(
        eventId,
        transferId,
        "TRANSFER_ACCEPTED",
        """
            {"eventId":"%s","transferId":"%s"}""".formatted(eventId, transferId),
        attempts,
        UUID.randomUUID());
  }
}
