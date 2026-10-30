package com.financialsimpleapp.transfer_service.transfer.adapter.out.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(
    name = "app.outbox.scheduling.enabled",
    havingValue = "true",
    matchIfMissing = true)
@Component
class OutboxRelayScheduler {

  private final OutboxRelay outboxRelay;

  OutboxRelayScheduler(OutboxRelay outboxRelay) {
    this.outboxRelay = outboxRelay;
  }

  @Scheduled(fixedDelayString = "${app.outbox.poll-delay-ms:1000}")
  public void relay() {
    outboxRelay.relayPendingEvents();
  }
}
