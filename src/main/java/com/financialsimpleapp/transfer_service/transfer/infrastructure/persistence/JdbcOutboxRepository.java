package com.financialsimpleapp.transfer_service.transfer.infrastructure.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.financialsimpleapp.transfer_service.transfer.application.port.OutboxRepository;

import java.util.UUID;

@Repository
public class JdbcOutboxRepository implements OutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcOutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
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
