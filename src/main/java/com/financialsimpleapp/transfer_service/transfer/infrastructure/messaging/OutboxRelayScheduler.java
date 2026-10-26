package com.financialsimpleapp.transfer_service.transfer.infrastructure.messaging;

import com.financialsimpleapp.transfer_service.transfer.application.port.in.RelayOutboxEventsUseCase;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(name = "app.outbox.scheduling.enabled", havingValue = "true", matchIfMissing = true)
@Component
public class OutboxRelayScheduler {

  private final RelayOutboxEventsUseCase relayOutboxEvents;

  public OutboxRelayScheduler(RelayOutboxEventsUseCase relayOutboxEvents) {
    this.relayOutboxEvents = relayOutboxEvents;
  }

  @Scheduled(fixedDelayString = "${app.outbox.poll-delay-ms:1000}")
  public void relay() {
    relayOutboxEvents.relayPendingEvents();
  }
}
