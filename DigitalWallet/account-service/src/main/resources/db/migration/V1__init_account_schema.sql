CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_number VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    kyc_checked BOOLEAN NOT NULL DEFAULT FALSE,
    transfer_limit NUMERIC,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_accounts_status CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED'))
);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload TEXT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_accounts_account_number ON accounts(account_number);
CREATE INDEX IF NOT EXISTS idx_outbox_messages_unprocessed ON outbox_messages (processed) WHERE processed = FALSE;