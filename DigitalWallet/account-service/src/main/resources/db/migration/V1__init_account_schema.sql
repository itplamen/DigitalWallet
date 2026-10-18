CREATE TABLE IF NOT EXISTS accounts (
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
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS inbox_messages (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    consumer VARCHAR(50) NOT NULL,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (id, consumer)
);

COMMENT ON COLUMN outbox_messages.id IS 'eventId';
COMMENT ON COLUMN outbox_messages.aggregate_id IS 'Kafka key';
COMMENT ON COLUMN outbox_messages.headers IS 'correlationId, traceparent';
COMMENT ON COLUMN inbox_messages.id IS 'eventId';

CREATE INDEX IF NOT EXISTS idx_accounts_account_number ON accounts(account_number);
CREATE INDEX IF NOT EXISTS idx_outbox_messages_unpublished ON outbox_messages (created_at) WHERE published_at IS NULL;