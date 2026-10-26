package com.financialsimpleapp.transfer_service.transfer.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.financialsimpleapp.transfer_service.transfer.application.port.in.ProcessTransferUseCase;
import java.io.IOException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransferAcceptedListener {

  private final ObjectMapper objectMapper;
  private final ProcessTransferUseCase processTransferUseCase;

  public TransferAcceptedListener(
      ObjectMapper objectMapper,
      ProcessTransferUseCase processTransferUseCase) {
    this.objectMapper = objectMapper;
    this.processTransferUseCase = processTransferUseCase;
  }

  @KafkaListener(
      topics = "${app.kafka.topics.transfer-accepted:transfer.accepted}",
      groupId = "${spring.kafka.consumer.group-id:transfer-processing}")
  public void onMessage(String payload) throws IOException {
    TransferAcceptedEvent event =
        objectMapper.readValue(payload, TransferAcceptedEvent.class);

    processTransferUseCase.process(event.transferId());
  }
}
