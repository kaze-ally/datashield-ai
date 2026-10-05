-- postgres/init/004_rca_reports.sql
-- Week 4 part 2: RCA Copilot persistence.

\c datashield

CREATE TABLE IF NOT EXISTS rca_reports (
    id                        BIGSERIAL PRIMARY KEY,
    rca_id                    UUID NOT NULL UNIQUE,
    monitor_name                TEXT NOT NULL,
    report_id                     TEXT,
    root_cause_category             TEXT NOT NULL,
    narrative                         TEXT NOT NULL,
    severity                            TEXT NOT NULL,   -- low | medium | high
    confidence                            DOUBLE PRECISION NOT NULL,
    model_used                              TEXT NOT NULL,
    outcome_success                           BOOLEAN NOT NULL,
    outcome_reason                              TEXT NOT NULL,
    context_snapshot                              JSONB,
    created_at                                      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rca_reports_monitor_time
    ON rca_reports (monitor_name, created_at DESC);

-- GRANT SELECT, INSERT ON rca_reports TO datashield_app;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO datashield_app;
