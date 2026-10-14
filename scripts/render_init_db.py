"""Idempotently initialize DataShield's Render Postgres database."""
from pathlib import Path
import os

import psycopg2


dsn = os.environ["POSTGRES_APP_DSN"]
init_dir = Path("/app/db-init")

base_schema = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS llm_semantic_cache (
    id BIGSERIAL PRIMARY KEY,
    prompt_text TEXT NOT NULL,
    embedding VECTOR(384) NOT NULL,
    response_text TEXT NOT NULL,
    model_used TEXT NOT NULL,
    hit_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_hit_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS llm_semantic_cache_embedding_idx
    ON llm_semantic_cache USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS data_catalog_entries (
    id BIGSERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT 'datashield',
    schema_fingerprint TEXT NOT NULL,
    schema_json JSONB NOT NULL,
    ai_description TEXT,
    ai_tags TEXT[],
    lineage_upstream TEXT[],
    lineage_downstream TEXT[],
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset_name, schema_fingerprint)
);

CREATE TABLE IF NOT EXISTS pipeline_events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    source_topic TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS validated_orders (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT,
    order_total DOUBLE PRECISION,
    item_count INTEGER,
    shipping_cost DOUBLE PRECISION,
    discount_pct DOUBLE PRECISION,
    payment_method TEXT,
    shipping_country TEXT,
    product_category TEXT,
    unit_price DOUBLE PRECISION,
    currency TEXT,
    customer_segment TEXT,
    region TEXT,
    shipping_method TEXT,
    device_type TEXT,
    order_channel TEXT,
    is_first_purchase BOOLEAN,
    loyalty_tier TEXT,
    warehouse_id TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO datashield;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO datashield;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO datashield;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO datashield;
"""


with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
    cursor.execute(base_schema)
    for path in sorted(init_dir.glob("*.sql")):
        sql = "\n".join(
            line for line in path.read_text(encoding="utf-8").splitlines()
            if not line.lstrip().startswith("\\c ")
        )
        if sql.strip():
            cursor.execute(sql)

print("DataShield Postgres schema is ready.")
