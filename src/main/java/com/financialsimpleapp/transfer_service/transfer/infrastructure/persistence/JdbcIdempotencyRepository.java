package com.financialsimpleapp.transfer_service.transfer.infrastructure.persistence;

import com.financialsimpleapp.transfer_service.transfer.application.port.IdempotencyRepository;
import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcIdempotencyRepository implements IdempotencyRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcIdempotencyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean tryCreate(
            UUID clientId,
            String idempotencyKey,
            String requestHash,
            UUID transferId
    ) {
        int inserted = jdbcTemplate.update("""
                INSERT INTO idempotency_records (
                    client_id,
                    idempotency_key,
                    request_hash,
                    transfer_id
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT (client_id, idempotency_key) DO NOTHING
                """,
                clientId,
                idempotencyKey,
                requestHash,
                transferId
        );

        return inserted == 1;
    }

    @Override
    public Optional<ExistingIdempotencyRecord> find(
            UUID clientId,
            String idempotencyKey
    ) {
        List<ExistingIdempotencyRecord> records = jdbcTemplate.query("""
                SELECT i.request_hash, i.transfer_id, t.status
                FROM idempotency_records i
                JOIN transfer_requests t ON t.id = i.transfer_id
                WHERE i.client_id = ?
                  AND i.idempotency_key = ?
                """,
                (rs, rowNum) -> new ExistingIdempotencyRecord(
                        rs.getString("request_hash").trim(),
                        rs.getObject("transfer_id", UUID.class),
                        TransferStatus.valueOf(rs.getString("status"))
                ),
                clientId,
                idempotencyKey
        );

        return records.stream().findFirst();
    }

    public record ExistingIdempotencyRecord(
            String requestHash,
            UUID transferId,
            TransferStatus status
    ) {
    }
}
