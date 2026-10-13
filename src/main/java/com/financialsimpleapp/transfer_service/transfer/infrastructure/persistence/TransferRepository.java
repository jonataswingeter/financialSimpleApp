package com.financialsimpleapp.transfer_service.transfer.infrastructure.persistence;

import com.financialsimpleapp.transfer_service.transfer.domain.Transfer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class TransferRepository {

    private final JdbcTemplate jdbcTemplate;

    public TransferRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(UUID clientId, Transfer transfer) {
        jdbcTemplate.update("""
                INSERT INTO transfer_requests (
                    id,
                    client_id,
                    from_account_id,
                    to_account_id,
                    amount,
                    currency,
                    status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                transfer.getId(),
                clientId,
                transfer.getFromAccountId(),
                transfer.getToAccountId(),
                transfer.getAmount(),
                transfer.getCurrency().getCurrencyCode(),
                transfer.getStatus().name()
        );
    }

    public void deleteById(UUID transferId) {
        jdbcTemplate.update("""
                DELETE FROM transfer_requests
                WHERE id = ?
                """,
                transferId
        );
    }
}