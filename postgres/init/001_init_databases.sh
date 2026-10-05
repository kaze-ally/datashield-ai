#!/usr/bin/env bash
# =============================================================================
# DataShield AI — Postgres bootstrap
# Runs automatically on first container start (docker-entrypoint-initdb.d).
# Creates:
#   1. `airflow`    -> Airflow metadata DB
#   2. `datashield` -> application DB (data contracts, anomaly log,
#                      LLM semantic cache, AI-generated data catalog)
# pgvector is enabled on `datashield` for the semantic cache (Innovation #2)
# and future embedding-based catalog search (Innovation #3).
# =============================================================================
set -euo pipefail

# Match the compose env names used by the rest of the stack so the DB users and
# service connection strings always agree. The default values keep previous
# behavior while allowing the project .env file to drive real credentials.
: "${DS_AIRFLOW_PASSWORD:=${POSTGRES_AIRFLOW_PASSWORD:-airflow}}"
: "${DS_APP_PASSWORD:=${POSTGRES_APP_PASSWORD:-datashield}}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE ROLE airflow WITH LOGIN PASSWORD '${DS_AIRFLOW_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;

    CREATE ROLE datashield WITH LOGIN PASSWORD '${DS_APP_PASSWORD}';
    CREATE DATABASE datashield OWNER datashield;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "datashield" <<-SQL
    CREATE EXTENSION IF NOT EXISTS vector;

    -- Semantic cache for the LLM Gateway (FrugalGPT-style cascade + cache)
    CREATE TABLE IF NOT EXISTS llm_semantic_cache (
        id              BIGSERIAL PRIMARY KEY,
        prompt_text     TEXT NOT NULL,
        embedding       VECTOR(384) NOT NULL,  -- all-MiniLM-L6-v2 dimension
        response_text   TEXT NOT NULL,
        model_used      TEXT NOT NULL,
        hit_count       INT NOT NULL DEFAULT 0,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_hit_at     TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS llm_semantic_cache_embedding_idx
        ON llm_semantic_cache USING hnsw (embedding vector_cosine_ops);

    -- AI-generated data catalog (OpenLineage events + LLM enrichment)
    CREATE TABLE IF NOT EXISTS data_catalog_entries (
        id                   BIGSERIAL PRIMARY KEY,
        dataset_name         TEXT NOT NULL,
        namespace            TEXT NOT NULL DEFAULT 'datashield',
        schema_fingerprint   TEXT NOT NULL,
        schema_json          JSONB NOT NULL,
        ai_description       TEXT,
        ai_tags              TEXT[],
        lineage_upstream     TEXT[],
        lineage_downstream   TEXT[],
        first_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (dataset_name, schema_fingerprint)
    );

    CREATE TABLE IF NOT EXISTS ai_catalog_dlq (
        id           BIGSERIAL PRIMARY KEY,
        event_payload JSONB NOT NULL,
        error_reason TEXT,
        failed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    -- Anomaly / drift / remediation audit trail
    CREATE TABLE IF NOT EXISTS pipeline_events (
        id            BIGSERIAL PRIMARY KEY,
        event_type    TEXT NOT NULL,   -- anomaly | drift | schema_heal | circuit_break | rca
        source_topic  TEXT,
        payload       JSONB,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    -- Drift-monitor sink table: the drift service reads and writes here using
    -- the datashield app user, so grant access explicitly to the role used by
    -- the FastAPI service and the seed scripts.
    CREATE TABLE IF NOT EXISTS validated_orders (
        id                 BIGSERIAL PRIMARY KEY,
        order_id           TEXT,
        order_total        DOUBLE PRECISION,
        item_count         INTEGER,
        shipping_cost      DOUBLE PRECISION,
        discount_pct       DOUBLE PRECISION,
        payment_method     TEXT,
        shipping_country   TEXT,
        product_category   TEXT,
        ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO datashield;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO datashield;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO datashield;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO datashield;
SQL

echo "[datashield-init] airflow + datashield databases provisioned."
