package com.financialsimpleapp.transfer_service.transfer.application.port.out;

public class EventPublishingException extends RuntimeException {

  public EventPublishingException(String message, Throwable cause) {
    super(message, cause);
  }
}
