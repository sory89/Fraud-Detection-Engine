-- ── All transactions ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    id             SERIAL PRIMARY KEY,
    transaction_id TEXT        NOT NULL,
    user_id        TEXT        NOT NULL,
    amount         NUMERIC(14,2),
    currency       TEXT,
    merchant       TEXT,
    country        TEXT,
    card_type      TEXT,
    card_last4     TEXT,
    ip_address     TEXT,
    velocity_flag  BOOLEAN     DEFAULT FALSE,
    timestamp      TIMESTAMPTZ,
    ingested_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tx_user    ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_tx_country ON transactions(country);
CREATE INDEX IF NOT EXISTS idx_tx_ts      ON transactions(timestamp DESC);

-- ── Fraud alerts ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id             SERIAL PRIMARY KEY,
    transaction_id TEXT        NOT NULL,
    user_id        TEXT,
    amount         NUMERIC(14,2),
    currency       TEXT,
    merchant       TEXT,
    country        TEXT,
    card_type      TEXT,
    card_last4     TEXT,
    ip_address     TEXT,
    fraud_reason   TEXT        NOT NULL,
    timestamp      TIMESTAMPTZ,
    ingested_at    TIMESTAMPTZ DEFAULT now(),
    acknowledged   BOOLEAN     DEFAULT FALSE,
    notes          TEXT
);

CREATE INDEX IF NOT EXISTS idx_fraud_reason  ON fraud_alerts(fraud_reason);
CREATE INDEX IF NOT EXISTS idx_fraud_country ON fraud_alerts(country);
CREATE INDEX IF NOT EXISTS idx_fraud_ts      ON fraud_alerts(ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_ack     ON fraud_alerts(acknowledged);

-- ── DLQ ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dlq_messages (
    id              SERIAL PRIMARY KEY,
    kafka_offset    BIGINT,
    kafka_partition INT,
    error_type      TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
