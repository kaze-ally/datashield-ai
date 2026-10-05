-- postgres/init/003_circuit_breaker.sql
-- Week 4: Remediation Circuit Breaker persistence.
-- Follows 001_init_databases.sh / 002_drift_reports.sql's convention —
-- runs against the datashield database. If 001 grants privileges to your
-- app role by name, add the same GRANTs here (see the Week 3 fix for the
-- exact pattern) rather than assuming they're inherited automatically.

\c datashield

CREATE TABLE IF NOT EXISTS circuit_breaker_state (
    monitor_name                TEXT PRIMARY KEY,
    state                        TEXT NOT NULL,              -- CLOSED | OPEN | HALF_OPEN
    consecutive_failures          INTEGER NOT NULL DEFAULT 0,
    trip_count                     INTEGER NOT NULL DEFAULT 0,
    opened_at                       TIMESTAMPTZ,
    cooldown_seconds                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    half_open_trial_in_flight         BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at                          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS circuit_breaker_events (
    id                        BIGSERIAL PRIMARY KEY,
    event_id                  UUID NOT NULL UNIQUE,
    monitor_name                TEXT NOT NULL,
    event_type                    TEXT NOT NULL,   -- incident_forwarded | incident_blocked | outcome_success | outcome_failure
    from_state                      TEXT,
    to_state                          TEXT NOT NULL,
    reason                              TEXT NOT NULL,
    related_report_id                    TEXT,
    created_at                              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cb_events_monitor_time
    ON circuit_breaker_events (monitor_name, created_at DESC);

-- Example grant pattern (uncomment and adjust to your actual app role name,
-- matching the fix already applied to validated_orders/drift_reports):
-- GRANT SELECT, INSERT, UPDATE ON circuit_breaker_state, circuit_breaker_events TO datashield_app;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO datashield_app;
