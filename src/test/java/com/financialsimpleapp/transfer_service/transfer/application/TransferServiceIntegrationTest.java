package com.financialsimpleapp.transfer_service.transfer.application;

import com.financialsimpleapp.transfer_service.transfer.domain.TransferStatus;
import com.financialsimpleapp.transfer_service.transfer.infrastructure.persistence.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

@SpringBootTest
@Testcontainers
class TransferServiceIntegrationTest {

    @Container
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:16-alpine")
                    .withDatabaseName("transfer_test")
                    .withUsername("test")
                    .withPassword("test");

    @DynamicPropertySource
    static void configureDatabase(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name",
                POSTGRES::getDriverClassName);
    }

    @Autowired
    private TransferService transferService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockitoSpyBean
    private OutboxRepository outboxRepository;

    private final UUID clientId = UUID.randomUUID();
    private final UUID fromAccountId = UUID.randomUUID();
    private final UUID toAccountId = UUID.randomUUID();

    private static final Currency EUR = Currency.getInstance("EUR");

    @BeforeEach
    void setUp() {
        reset(outboxRepository);

        jdbcTemplate.update("DELETE FROM outbox_events");
        jdbcTemplate.update("DELETE FROM ledger_entries");
        jdbcTemplate.update("DELETE FROM idempotency_records");
        jdbcTemplate.update("DELETE FROM transfer_requests");
        jdbcTemplate.update("DELETE FROM accounts");

        insertAccount(fromAccountId, new BigDecimal("1000.00"));
        insertAccount(toAccountId, new BigDecimal("500.00"));
    }

    @Test
    void shouldCreateTransferIdempotencyRecordAndOutboxEvent() {
        TransferAcceptanceResult result = accept(
                "request-001",
                new BigDecimal("25.00")
        );

        assertNotNull(result.transferId());
        assertEquals(TransferStatus.ACCEPTED, result.status());
        assertFalse(result.replayed());

        assertEquals(1, countTransfers());
        assertEquals(1, countIdempotencyRecords());
        assertEquals(1, countOutboxEvents());

        String storedStatus = jdbcTemplate.queryForObject(
                "SELECT status FROM transfer_requests WHERE id = ?",
                String.class,
                result.transferId()
        );

        assertEquals("ACCEPTED", storedStatus);
    }

    @Test
    void shouldReturnOriginalTransferForIdenticalRetry() {
        TransferAcceptanceResult first = accept(
                "request-002",
                new BigDecimal("25.00")
        );

        TransferAcceptanceResult retry = accept(
                "request-002",
                new BigDecimal("25.00")
        );

        assertEquals(first.transferId(), retry.transferId());
        assertEquals(TransferStatus.ACCEPTED, retry.status());
        assertFalse(first.replayed());
        assertTrue(retry.replayed());

        assertEquals(1, countTransfers());
        assertEquals(1, countIdempotencyRecords());
        assertEquals(1, countOutboxEvents());
    }

    @Test
    void shouldRejectSameKeyWithDifferentRequestAndRollbackCandidate() {
        TransferAcceptanceResult first = accept(
                "request-003",
                new BigDecimal("25.00")
        );

        assertThrows(
                IdempotencyConflictException.class,
                () -> accept("request-003", new BigDecimal("30.00"))
        );

        assertEquals(1, countTransfers());
        assertEquals(1, countIdempotencyRecords());
        assertEquals(1, countOutboxEvents());

        UUID persistedTransferId = jdbcTemplate.queryForObject("""
                SELECT transfer_id
                FROM idempotency_records
                WHERE client_id = ?
                  AND idempotency_key = ?
                """,
                UUID.class,
                clientId,
                "request-003"
        );

        assertEquals(first.transferId(), persistedTransferId);
    }

    @Test
    void shouldRollbackAllWritesWhenOutboxInsertFails() {
        doThrow(new IllegalStateException("Simulated outbox failure"))
                .when(outboxRepository)
                .insertTransferAccepted(any(UUID.class), any(UUID.class));

        assertThrows(
                IllegalStateException.class,
                () -> accept("request-004", new BigDecimal("40.00"))
        );

        assertEquals(0, countTransfers());
        assertEquals(0, countIdempotencyRecords());
        assertEquals(0, countOutboxEvents());
    }

    private TransferAcceptanceResult accept(
            String idempotencyKey,
            BigDecimal amount
    ) {
        return transferService.acceptTransfer(
                clientId,
                idempotencyKey,
                fromAccountId,
                toAccountId,
                amount,
                EUR
        );
    }

    private void insertAccount(UUID accountId, BigDecimal balance) {
        jdbcTemplate.update("""
                INSERT INTO accounts (
                    id,
                    currency,
                    balance,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (?, 'EUR', ?, 'ACTIVE', CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP)
                """,
                accountId,
                balance
        );
    }

    private int countTransfers() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM transfer_requests",
                Integer.class
        );
    }

    private int countIdempotencyRecords() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM idempotency_records",
                Integer.class
        );
    }

    private int countOutboxEvents() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events",
                Integer.class
        );
    }
}
