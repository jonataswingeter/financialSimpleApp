package com.financialsimpleapp.transfer_service.transfer.adapter.out.outbox;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The relay's side of the outbox table: claiming due events under a lease, and recording the
 * outcome of a send.
 *
 * <p>Each method is its own short transaction, so the relay never calls the broker while a
 * transaction is open. A lease is identified by its lock token: an instance that lost its lease
 * cannot overwrite the new owner's outcome.
 */
@Repository
class JdbcOutboxRelayStore {

  private static final String CLAIM_PENDING =
      """
      WITH candidates AS (
          SELECT event_id
          FROM outbox_events
          WHERE published_at IS NULL
            AND next_attempt_at <= clock_timestamp()
            AND (
                locked_until IS NULL
                OR locked_until <= clock_timestamp()
            )
          ORDER BY created_at, event_id
          LIMIT ?
          FOR UPDATE SKIP LOCKED
      )
      UPDATE outbox_events AS o
      SET locked_until =
              clock_timestamp() + (? * interval '1 millisecond'),
          lock_token = gen_random_uuid()
      FROM candidates AS c
      WHERE o.event_id = c.event_id
      RETURNING
          o.event_id,
          o.aggregate_id,
          o.event_type,
          o.payload::text AS payload,
          o.attempts,
          o.lock_token
      """;

  private static final String MARK_PUBLISHED =
      """
      UPDATE outbox_events
      SET published_at = clock_timestamp(),
          locked_until = NULL,
          lock_token = NULL
      WHERE event_id = ?
        AND lock_token = ?
        AND published_at IS NULL
      """;

  private static final String SCHEDULE_RETRY =
      """
      UPDATE outbox_events
      SET attempts = attempts + 1,
          next_attempt_at =
              clock_timestamp() + (? * interval '1 millisecond'),
          locked_until = NULL,
          lock_token = NULL
      WHERE event_id = ?
        AND lock_token = ?
        AND published_at IS NULL
      """;

  private final JdbcTemplate jdbcTemplate;

  JdbcOutboxRelayStore(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Transactional
  public List<ClaimedOutboxEvent> claimPending(int batchSize, Duration lockDuration) {

    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }

    long lockMillis = positiveMillis(lockDuration, "lockDuration");

    return jdbcTemplate.query(
        CLAIM_PENDING,
        (rs, rowNum) ->
            new ClaimedOutboxEvent(
                rs.getObject("event_id", UUID.class),
                rs.getObject("aggregate_id", UUID.class),
                rs.getString("event_type"),
                rs.getString("payload"),
                rs.getInt("attempts"),
                rs.getObject("lock_token", UUID.class)),
        batchSize,
        lockMillis);
  }

  @Transactional
  public boolean markPublished(UUID eventId, UUID lockToken) {

    return jdbcTemplate.update(MARK_PUBLISHED, eventId, lockToken) == 1;
  }

  @Transactional
  public boolean scheduleRetry(UUID eventId, UUID lockToken, Duration delay) {

    long delayMillis = positiveMillis(delay, "delay");

    return jdbcTemplate.update(SCHEDULE_RETRY, delayMillis, eventId, lockToken) == 1;
  }

  private static long positiveMillis(Duration duration, String parameterName) {

    if (duration == null || duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(parameterName + " must be positive");
    }

    long millis = duration.toMillis();

    if (millis <= 0) {
      throw new IllegalArgumentException(parameterName + " must be at least 1 millisecond");
    }

    return millis;
  }
}
