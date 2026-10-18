CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL,
    balance NUMERIC(19, 8) NOT NULL CHECK (balance >= 0),
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uq_wallets_account_currency UNIQUE (account_id, currency),
    CONSTRAINT chk_wallets_currency_format CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE IF NOT EXISTS wallet_events (
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
    version BIGINT NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (wallet_id, version)
);

CREATE TABLE IF NOT EXISTS reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
    payment_id UUID NOT NULL UNIQUE,
    amount NUMERIC(19, 8) NOT NULL CHECK (amount > 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('ACTIVE', 'CAPTURED', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ
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

CREATE INDEX IF NOT EXISTS idx_outbox_messages_unpublished ON outbox_messages (created_at) WHERE published_at IS NULL;
