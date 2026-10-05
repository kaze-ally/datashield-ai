CREATE TABLE IF NOT EXISTS ai_catalog_dlq (
    id            BIGSERIAL PRIMARY KEY,
    event_payload JSONB NOT NULL,
    error_reason  TEXT,
    failed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
