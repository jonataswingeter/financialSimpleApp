package com.financialsimpleapp.transfer_service.transfer.application.service;

import java.time.Duration;

public record OutboxRelaySettings(
    int batchSize,
    Duration lockDuration,
    Duration sendTimeout,
    Duration retryBase,
    Duration retryMax) {

  public OutboxRelaySettings {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    if (lockDuration.isZero() || lockDuration.isNegative()) {
      throw new IllegalArgumentException("lockDuration must be positive");
    }
    if (sendTimeout.isZero() || sendTimeout.isNegative()) {
      throw new IllegalArgumentException("sendTimeout must be positive");
    }
    if (retryBase.isZero() || retryBase.isNegative()
        || retryMax.compareTo(retryBase) < 0) {
      throw new IllegalArgumentException("Invalid retry durations");
    }
  }

  /**
   * The lease must outlast sending a whole batch, otherwise another
   * instance could claim events that are still being published.
   */
  public Duration leaseDuration() {
    Duration minimumLease = sendTimeout
        .multipliedBy(batchSize)
        .plusSeconds(30);

    return lockDuration.compareTo(minimumLease) >= 0
        ? lockDuration
        : minimumLease;
  }
}
