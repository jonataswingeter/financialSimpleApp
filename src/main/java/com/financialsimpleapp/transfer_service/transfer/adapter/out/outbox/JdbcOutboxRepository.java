package com.financialsimpleapp.transfer_service.transfer.adapter.out.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.financialsimpleapp.transfer_service.transfer.application.port.out.OutboxRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferAcceptedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** The application's side of the outbox: stores events in the caller's transaction. */
@Repository
public class JdbcOutboxRepository implements OutboxRepository {

  private static final String INSERT_EVENT =
      """
      INSERT INTO outbox_events (
          event_id,
          aggregate_id,
          event_type,
          payload,
          created_at,
          published_at,
          attempts,
          next_attempt_at,
          locked_until,
          lock_token
      )
      VALUES (
          ?, ?, ?, CAST(? AS JSONB),
          now(), NULL, 0, now(), NULL, NULL
      )
      """;

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;

  public JdbcOutboxRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
  }

  @Override
  public void append(TransferAcceptedEvent event) {
    jdbcTemplate.update(
        INSERT_EVENT,
        event.eventId(),
        event.transferId(),
        TransferAcceptedEvent.TYPE,
        toJson(event));
  }

  private String toJson(Object event) {
    try {
      return objectMapper.writeValueAsString(event);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Could not serialize outbox event", exception);
    }
  }
}
