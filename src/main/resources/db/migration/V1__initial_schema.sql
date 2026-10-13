
-- V1__initial_schema.sql
-- Initial schema for asynchronous internal bank transfers.

-- ============================================================
-- 1. Accounts
-- ============================================================

CREATE TABLE accounts (
    id              UUID PRIMARY KEY,
    currency        CHAR(3) NOT NULL,
    balance         NUMERIC(19, 2) NOT NULL DEFAULT 0,
    status          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_accounts_balance_non_negative
        CHECK (balance >= 0),

    CONSTRAINT chk_accounts_currency
        CHECK (currency ~ '^[A-Z]{3}$'),

    CONSTRAINT chk_accounts_status
        CHECK (status IN ('ACTIVE', 'BLOCKED', 'CLOSED'))
);


-- ============================================================
-- 2. Transfer requests
-- ============================================================

CREATE TABLE transfer_requests (
    id                  UUID PRIMARY KEY,
    client_id           UUID NOT NULL,
    from_account_id     UUID NOT NULL REFERENCES accounts(id),
    to_account_id       UUID NOT NULL REFERENCES accounts(id),
    amount              NUMERIC(19, 2) NOT NULL,
    currency            CHAR(3) NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'ACCEPTED',
    rejection_code      VARCHAR(50),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,

    CONSTRAINT chk_transfer_amount_positive
        CHECK (amount > 0),

    CONSTRAINT chk_transfer_different_accounts
        CHECK (from_account_id <> to_account_id),

    CONSTRAINT chk_transfer_currency
        CHECK (currency ~ '^[A-Z]{3}$'),

    CONSTRAINT chk_transfer_status
        CHECK (
            status IN (
                'ACCEPTED',
                'PROCESSING',
                'COMPLETED',
                'REJECTED'
            )
        ),

    CONSTRAINT chk_transfer_completion
        CHECK (
            (status = 'COMPLETED' AND completed_at IS NOT NULL)
            OR
            (status <> 'COMPLETED' AND completed_at IS NULL)
        )
);

CREATE INDEX idx_transfer_requests_status_created
    ON transfer_requests (status, created_at);

CREATE INDEX idx_transfer_requests_from_account
    ON transfer_requests (from_account_id, created_at);

CREATE INDEX idx_transfer_requests_to_account
    ON transfer_requests (to_account_id, created_at);


-- ============================================================
-- 3. Idempotency records
-- ============================================================

CREATE TABLE idempotency_records (
    client_id           UUID NOT NULL,
    idempotency_key     VARCHAR(255) NOT NULL,
    request_hash        CHAR(64) NOT NULL,
    transfer_id         UUID NOT NULL UNIQUE
                            REFERENCES transfer_requests(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_idempotency_records
        PRIMARY KEY (client_id, idempotency_key),

    CONSTRAINT chk_idempotency_key_not_blank
        CHECK (LENGTH(TRIM(idempotency_key)) > 0),

    CONSTRAINT chk_idempotency_hash
        CHECK (request_hash ~ '^[0-9a-f]{64}$')
);


-- ============================================================
-- 4. Immutable financial ledger
-- ============================================================

CREATE TABLE ledger_entries (
    id                  UUID PRIMARY KEY,
    transfer_id         UUID NOT NULL REFERENCES transfer_requests(id),
    account_id          UUID NOT NULL REFERENCES accounts(id),
    entry_type          VARCHAR(10) NOT NULL,
    amount              NUMERIC(19, 2) NOT NULL,
    currency            CHAR(3) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_ledger_entry_type
        CHECK (entry_type IN ('DEBIT', 'CREDIT')),

    CONSTRAINT chk_ledger_entry_sign
        CHECK (
            (entry_type = 'DEBIT' AND amount < 0)
            OR
            (entry_type = 'CREDIT' AND amount > 0)
        ),

    CONSTRAINT uq_ledger_transfer_entry_type
        UNIQUE (transfer_id, entry_type)
);

CREATE INDEX idx_ledger_account_created
    ON ledger_entries (account_id, created_at, id);


-- ============================================================
-- 5. Transactional outbox
-- ============================================================

CREATE TABLE outbox_events (
    event_id            UUID PRIMARY KEY,
    aggregate_id        UUID NOT NULL,
    event_type          VARCHAR(100) NOT NULL,
    payload             JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at        TIMESTAMPTZ,
    attempts            INTEGER NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until        TIMESTAMPTZ,
    lock_token          UUID,

    CONSTRAINT chk_outbox_attempts
        CHECK (attempts >= 0),

    CONSTRAINT chk_outbox_lock
        CHECK (
            (locked_until IS NULL AND lock_token IS NULL)
            OR
            (locked_until IS NOT NULL AND lock_token IS NOT NULL)
        )
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (next_attempt_at, created_at)
    WHERE published_at IS NULL;
