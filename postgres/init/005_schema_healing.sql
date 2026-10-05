-- postgres/init/005_schema_healing.sql
--
-- Only auto-runs on a FRESH postgres volume (docker-entrypoint-initdb.d
-- behavior — same caveat CLAUDE_HANDOFF_REPORT.md hit for 002-004). Your
-- postgres volume already exists, so apply this manually, same as the
-- others were:
--
--   Get-Content postgres/init/005_schema_healing.sql | docker compose exec -T postgres psql -U <your_pg_user> -d <your_pg_db>

CREATE TABLE IF NOT EXISTS schema_healing_reports (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id       UUID NOT NULL,
    contract_name   TEXT NOT NULL,
    success         BOOLEAN NOT NULL,
    reason          TEXT NOT NULL,
    root_cause_category TEXT NOT NULL,
    narrative       TEXT,
    confidence      NUMERIC(4, 3) NOT NULL,
    model_used      TEXT,
    ops_applied     JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_schema_healing_reports_contract_time
    ON schema_healing_reports (contract_name, created_at DESC);

CREATE TABLE IF NOT EXISTS schema_healer_circuit_state (
    contract_name         TEXT PRIMARY KEY,
    consecutive_failures  INTEGER NOT NULL DEFAULT 0,
    paused_until          TIMESTAMPTZ,
    trip_count            INTEGER NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Same grant fix as Weeks 3-4 — but note the nuance the live test surfaced:
-- grants only matter if this file runs via docker-entrypoint-initdb.d on a
-- FRESH volume, where Postgres executes it as the postgres superuser and a
-- separate app role then needs explicit grants. If you apply it manually
-- as your app role itself (as the live test did:
-- `psql -U datashield -d datashield < 005_schema_healing.sql`), that role
-- CREATEs and therefore already OWNs these two tables — no GRANT needed
-- in that path. Uncomment below only if you later let this run as
-- postgres on a fresh volume instead:
--
-- GRANT SELECT, INSERT, UPDATE ON schema_healing_reports TO <your_app_role>;
-- GRANT SELECT, INSERT, UPDATE ON schema_healer_circuit_state TO <your_app_role>;
