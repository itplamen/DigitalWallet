CREATE TABLE IF NOT EXISTS wallets (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE,
    balance NUMERIC(19, 8) NOT NULL CHECK (balance >= 0),
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_wallets_currency_format CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload TEXT NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    version BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_unprocessed ON outbox_messages (processed) WHERE processed = FALSE;

CREATE TABLE ledger_entries (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    wallet_id BIGINT NOT NULL,
    transfer_id BIGINT,
    entry_type VARCHAR(20) NOT NULL,
    amount NUMERIC(19, 8) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_ledger_entries_wallet FOREIGN KEY (wallet_id) REFERENCES wallets(id) ON DELETE RESTRICT,
    CONSTRAINT chk_ledger_entry_type CHECK (entry_type IN ('OPENING', 'DEBIT', 'CREDIT')),
    CONSTRAINT uq_ledger_transfer_entry_type UNIQUE (transfer_id, entry_type),
    CONSTRAINT chk_ledger_entry_transfer
        CHECK (
            (entry_type = 'OPENING' AND transfer_id IS NULL)
            OR (entry_type IN ('DEBIT', 'CREDIT') AND transfer_id IS NOT NULL)
        )
);

CREATE INDEX idx_ledger_entries_wallet_created ON ledger_entries (wallet_id, created_at, id);

CREATE TABLE transfers (
    transfer_id BIGINT PRIMARY KEY,
    status VARCHAR(20) NOT NULL CHECK (status IN ('COMPLETED', 'FAILED')),
    reason VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT chk_transfer_result_reason
        CHECK ((status = 'COMPLETED' AND reason IS NULL)
            OR (status = 'FAILED' AND reason IS NOT NULL))
);