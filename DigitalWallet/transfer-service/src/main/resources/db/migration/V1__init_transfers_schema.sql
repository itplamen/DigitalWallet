CREATE TABLE IF NOT EXISTS transfers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_account_id UUID NOT NULL,
    target_account_id UUID NOT NULL,
    amount NUMERIC(19, 8) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_transfers_status CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED'))
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transfer_id UUID NOT NULL,
    from_status VARCHAR(20),
    to_status VARCHAR(20) NOT NULL,
    reason VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_audit_logs_transfer FOREIGN KEY (transfer_id) REFERENCES transfers(id) ON DELETE CASCADE
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
    transfer_id UUID REFERENCES transfers(id),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

COMMENT ON COLUMN outbox_messages.id IS 'eventId';
COMMENT ON COLUMN outbox_messages.aggregate_id IS 'Kafka key';
COMMENT ON COLUMN outbox_messages.headers IS 'correlationId, traceparent';
COMMENT ON COLUMN inbox_messages.id IS 'eventId';

CREATE INDEX IF NOT EXISTS idx_audit_logs_transfer_id ON audit_logs (transfer_id);
CREATE INDEX IF NOT EXISTS idx_outbox_messages_unpublished
    ON outbox_messages (created_at)
    WHERE published_at IS NULL;