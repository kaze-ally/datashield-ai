-- postgres/init/002_drift_reports.sql
-- Week 3: Drift report persistence table.
-- Numbered to run after 001_init_databases.sh via the official Postgres
-- image's docker-entrypoint-initdb.d mechanism (runs .sh and .sql files in
-- lexical order on first container start). If your datashield DB and
-- pgvector extension are created by 001, this assumes it runs afterward
-- and connects to the same database — adjust the \c target below if needed.

\c datashield

CREATE TABLE IF NOT EXISTS drift_reports (
    id                       BIGSERIAL PRIMARY KEY,
    report_id                UUID NOT NULL UNIQUE,
    monitor_name              TEXT NOT NULL,
    run_timestamp              TIMESTAMPTZ NOT NULL DEFAULT now(),
    reference_start            TIMESTAMPTZ NOT NULL,
    reference_end               TIMESTAMPTZ NOT NULL,
    current_start                TIMESTAMPTZ NOT NULL,
    current_end                   TIMESTAMPTZ NOT NULL,
    dataset_drift_detected        BOOLEAN NOT NULL,
    drift_share                    DOUBLE PRECISION NOT NULL,
    column_results                 JSONB NOT NULL,   -- per-column PSI/chi2 scores from the manual verifier
    evidently_raw                   JSONB,            -- full Evidently report dict, nullable
    html_report_path                TEXT,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT now()
);

GRANT ALL PRIVILEGES ON TABLE drift_reports TO datashield;
GRANT ALL PRIVILEGES ON SEQUENCE drift_reports_id_seq TO datashield;

GRANT ALL PRIVILEGES ON TABLE drift_reports TO datashield;
GRANT ALL PRIVILEGES ON SEQUENCE drift_reports_id_seq TO datashield;

CREATE INDEX IF NOT EXISTS idx_drift_reports_monitor_time
    ON drift_reports (monitor_name, run_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_drift_reports_detected
    ON drift_reports (dataset_drift_detected)
    WHERE dataset_drift_detected = TRUE;
