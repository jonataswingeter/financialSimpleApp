package com.financialsimpleapp.transfer_service.transfer.infrastructure.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    public OutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertTransferAccepted(
            UUID eventId,
            UUID transferId
    ) {
        String payload = """
                {"transferId":"%s"}
                """.formatted(transferId);

        jdbcTemplate.update("""
                INSERT INTO outbox_events (
                    event_id,
                    aggregate_id,
                    event_type,
                    payload
                )
                VALUES (?, ?, ?, CAST(? AS JSONB))
                """,
                eventId,
                transferId,
                "TransferAccepted",
                payload
        );
    }
}
