package com.financialsimpleapp.transfer_service.config;

import com.financialsimpleapp.transfer_service.transfer.application.service.OutboxRelaySettings;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OutboxRelayConfig {

  @Bean
  public OutboxRelaySettings outboxRelaySettings(
      @Value("${app.outbox.batch-size:100}") int batchSize,
      @Value("${app.outbox.lock-duration:PT30S}") Duration lockDuration,
      @Value("${app.outbox.send-timeout:PT10S}") Duration sendTimeout,
      @Value("${app.outbox.retry-base:PT1S}") Duration retryBase,
      @Value("${app.outbox.retry-max:PT5M}") Duration retryMax) {
    return new OutboxRelaySettings(batchSize, lockDuration, sendTimeout, retryBase, retryMax);
  }
}
