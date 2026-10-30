package com.financialsimpleapp.transfer_service.transfer.adapter.out.outbox;

public class EventPublishingException extends RuntimeException {

  public EventPublishingException(String message, Throwable cause) {
    super(message, cause);
  }
}
