CREATE TABLE recipients (
    account_id UUID PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(255),
    updated_at TIMESTAMPTZ
);

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL,
    channel VARCHAR(20) NOT NULL,
    template VARCHAR(255) NOT NULL,
    event_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,
    error VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    CONSTRAINT fk_notifications_recipient FOREIGN KEY (account_id) REFERENCES recipients(account_id),
    CONSTRAINT uq_notifications_event_channel_account UNIQUE (event_id, channel, account_id),
    CONSTRAINT chk_notifications_channel CHECK (channel IN ('EMAIL', 'SSE')),
    CONSTRAINT chk_notifications_status CHECK (status IN ('FAILED', 'SENT'))
);

CREATE TABLE IF NOT EXISTS inbox_messages (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    consumer VARCHAR(50) NOT NULL,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (id, consumer)
);

COMMENT ON COLUMN inbox_messages.id IS 'eventId';
