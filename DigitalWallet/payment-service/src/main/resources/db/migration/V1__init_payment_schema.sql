CREATE TABLE payments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL,
    type VARCHAR(20) NOT NULL,
    amount NUMERIC NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(30) NOT NULL,
    external_reference VARCHAR(255) UNIQUE,
    reason VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_payments_type CHECK (type IN ('DEPOSIT', 'WITHDRAWAL')),
    CONSTRAINT chk_payments_status CHECK (status IN ('PENDING', 'FUNDS_RESERVED', 'COMPLETED', 'FAILED', 'REFUNDED'))
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

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key VARCHAR PRIMARY KEY,
    request_hash CHAR(64) NOT NULL,
    response_status INTEGER,
    response_body JSONB,
    payment_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

COMMENT ON COLUMN outbox_messages.id IS 'eventId';
COMMENT ON COLUMN outbox_messages.aggregate_id IS 'Kafka key';
COMMENT ON COLUMN outbox_messages.headers IS 'correlationId, traceparent';
COMMENT ON COLUMN inbox_messages.id IS 'eventId';

CREATE INDEX IF NOT EXISTS idx_outbox_messages_unpublished ON outbox_messages (created_at) WHERE published_at IS NULL;
