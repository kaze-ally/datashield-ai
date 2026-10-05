"""
DataShield AI — AI-Enriched Data Catalog (Innovation #3)
=========================================================
Traditional catalogs (Marquez, plain OpenLineage consumers) capture *where
data comes from* but leave the *what does this field mean* work to humans.
This service closes that gap for DataShield:

  1. An Airflow OpenLineage listener (Week 2: enable
     `apache-airflow-providers-openlineage` with a Kafka transport) emits run/
     job/dataset lineage events onto the `openlineage-events` topic.
  2. This consumer also watches `schema-events` — a lightweight topic the
     sidecar validator (Week 2) publishes to only when a contract's schema
     fingerprint changes, so catalog enrichment never competes with the
     high-volume `validated-data` business-record traffic.
  3. On a new or changed schema, it asks the shared LLM Gateway
     (`/services/llm_gateway`) to draft a human-readable description and
     tags from the field names/types/data-contract metadata — the same
     "ML-driven metadata" pattern used by DataHub's auto-suggest features,
     but scoped to our own YAML data contracts instead of a general crawler.
  4. Results are upserted into `data_catalog_entries` in Postgres, which the
     Streamlit dashboard reads directly.

Week 1 scope: the consumer loop, DB upsert, and LLM Gateway call are real;
Airflow DAGs publish OpenLineage-compatible completion events to Kafka, and
this service folds those dataset edges into the catalog's lineage fields.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any

import psycopg2
import requests
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
LINEAGE_TOPIC = os.environ.get("LINEAGE_TOPIC", "openlineage-events")
SCHEMA_TOPIC = os.environ.get("SCHEMA_TOPIC", "schema-events")
POSTGRES_APP_DSN = os.environ.get("POSTGRES_APP_DSN", "")
LLM_GATEWAY_URL = os.environ.get("LLM_GATEWAY_URL", "http://llm-gateway:8000")
CONSUMER_RECONNECT_SECONDS = 5
CONSUMER_POLL_TIMEOUT_MS = 1000
MAX_ATTEMPTS = 4
BASE_DELAY = 1.5

logger = logging.getLogger(__name__)
_consumer_stop_event = threading.Event()
_consumer_thread: threading.Thread | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _consumer_thread, _consumer_stop_event
    _consumer_stop_event = threading.Event()
    _consumer_thread = threading.Thread(
        target=consume_loop,
        args=(_consumer_stop_event,),
        daemon=True,
        name="ai-catalog-consumer",
    )
    _consumer_thread.start()
    try:
        yield
    finally:
        _consumer_stop_event.set()
        _consumer_thread.join(timeout=10)


app = FastAPI(title="DataShield AI Catalog", lifespan=lifespan)


def get_db_conn():
    return psycopg2.connect(POSTGRES_APP_DSN)


def fingerprint(schema: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(schema, sort_keys=True).encode()).hexdigest()[:16]


def enrich_with_llm(dataset_name: str, schema: dict[str, Any]) -> tuple[str | None, list[str]]:
    prompt = (
        f"Given this dataset schema for '{dataset_name}':\n{json.dumps(schema, indent=2)}\n\n"
        "In under 40 words, describe what this dataset represents for a data catalog. "
        "Then on a new line, list 3-5 short lowercase tags separated by commas."
    )
    try:
        resp = requests.post(
            f"{LLM_GATEWAY_URL}/invoke",
            json={"prompt": prompt, "agent": "ai_catalog", "max_tokens": 200},
            timeout=(5, 120),
        )
        resp.raise_for_status()
        text = resp.json()["response"]
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        description = lines[0] if lines else ""
        tag_line = lines[-1] if len(lines) > 1 else ""
        if tag_line.lower().startswith("tags:"):
            tag_line = tag_line.split(":", maxsplit=1)[1]
        tags = [tag.strip() for tag in tag_line.split(",") if tag.strip()]
        return description, tags
    except requests.RequestException as exc:
        logger.warning("LLM Gateway request failed while enriching %s: %s", dataset_name, exc)
        return None, []


def write_to_dlq(event: Any, reason: str | None = None) -> None:
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ai_catalog_dlq (event_payload, error_reason)
            VALUES (%s, %s)
            """,
            (json.dumps(event), reason),
        )
        conn.commit()


def persist_with_retry(
    event: dict[str, Any],
    persist_fn: Callable[[dict[str, Any]], None],
) -> bool:
    last_err: Exception | None = None
    attempts = 0
    for attempt in range(MAX_ATTEMPTS):
        attempts = attempt + 1
        try:
            persist_fn(event)
            return True
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            last_err = exc
            if attempt < MAX_ATTEMPTS - 1:
                time.sleep(BASE_DELAY * (2 ** attempt))
        except Exception as exc:
            last_err = exc
            break
    logger.warning(
        "AI Catalog persist failed for event %s after %s attempts: %s",
        event,
        attempts,
        last_err,
    )
    write_to_dlq(event, reason=str(last_err))
    return False


def upsert_catalog_entry(dataset_name: str, schema: dict[str, Any]) -> None:
    fp = fingerprint(schema)
    description, tags = enrich_with_llm(dataset_name, schema)
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_catalog_entries
                (dataset_name, schema_fingerprint, schema_json, ai_description, ai_tags)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dataset_name, schema_fingerprint)
            DO UPDATE SET
                schema_json = EXCLUDED.schema_json,
                ai_description = CASE
                    WHEN EXCLUDED.ai_description IS NOT NULL THEN EXCLUDED.ai_description
                    WHEN data_catalog_entries.ai_description LIKE '(description pending%%' THEN NULL
                    ELSE data_catalog_entries.ai_description
                END,
                ai_tags = CASE
                    WHEN EXCLUDED.ai_description IS NOT NULL THEN EXCLUDED.ai_tags
                    WHEN data_catalog_entries.ai_description LIKE '(description pending%%' THEN ARRAY[]::TEXT[]
                    ELSE data_catalog_entries.ai_tags
                END,
                last_updated_at = now()
            """,
            (dataset_name, fp, json.dumps(schema), description, tags),
        )
        conn.commit()


def _lineage_dataset_names(datasets: Any) -> list[str]:
    if not isinstance(datasets, list):
        return []
    names = []
    for dataset in datasets:
        if not isinstance(dataset, dict):
            continue
        name = dataset.get("name")
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return sorted(set(names))


def persist_lineage_event(event: dict[str, Any]) -> None:
    """Merge OpenLineage input/output relationships into known catalog rows."""
    inputs = _lineage_dataset_names(event.get("inputs"))
    outputs = _lineage_dataset_names(event.get("outputs"))
    if not inputs and not outputs:
        raise ValueError("OpenLineage event must contain at least one named input or output")

    with get_db_conn() as conn, conn.cursor() as cur:
        for output in outputs:
            cur.execute(
                """
                UPDATE data_catalog_entries
                SET lineage_upstream = ARRAY(
                        SELECT DISTINCT name
                        FROM unnest(COALESCE(lineage_upstream, ARRAY[]::TEXT[]) || %s::TEXT[]) AS name
                    ), last_updated_at = now()
                WHERE dataset_name = %s
                """,
                (inputs, output),
            )
        for input_name in inputs:
            cur.execute(
                """
                UPDATE data_catalog_entries
                SET lineage_downstream = ARRAY(
                        SELECT DISTINCT name
                        FROM unnest(COALESCE(lineage_downstream, ARRAY[]::TEXT[]) || %s::TEXT[]) AS name
                    ), last_updated_at = now()
                WHERE dataset_name = %s
                """,
                (outputs, input_name),
            )
        conn.commit()


def consume_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                SCHEMA_TOPIC,
                LINEAGE_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=None,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                group_id="ai-catalog-consumer",
            )
            logger.info(
                "AI Catalog consumer connected to schema=%s lineage=%s",
                SCHEMA_TOPIC,
                LINEAGE_TOPIC,
            )
            for message in consumer:
                if stop_event.is_set():
                    break
                try:
                    payload = json.loads(message.value.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    logger.warning(
                        "Invalid catalog event from %s at partition=%s offset=%s; routing to DLQ: %s",
                        getattr(message, "topic", SCHEMA_TOPIC),
                        message.partition,
                        message.offset,
                        exc,
                    )
                    write_to_dlq(
                        {"raw_value": message.value.decode("utf-8", errors="replace")},
                        reason=f"invalid JSON catalog event: {exc}",
                    )
                    consumer.commit()
                    continue

                if getattr(message, "topic", SCHEMA_TOPIC) == LINEAGE_TOPIC:
                    if not isinstance(payload, dict):
                        write_to_dlq(payload, reason="OpenLineage event must be a JSON object")
                    elif payload.get("eventType") != "COMPLETE":
                        # START/RUNNING/ABORT events are valid lifecycle events,
                        # but only successful completion carries catalog edges.
                        consumer.commit()
                        continue
                    else:
                        try:
                            persisted = persist_with_retry(payload, persist_lineage_event)
                        except Exception:
                            logger.exception(
                                "Failed to persist or dead-letter lineage event at "
                                "partition=%s offset=%s; leaving offset uncommitted",
                                message.partition,
                                message.offset,
                            )
                            raise
                        if not persisted:
                            logger.error(
                                "Lineage event at partition=%s offset=%s was routed to DLQ",
                                message.partition,
                                message.offset,
                            )
                    consumer.commit()
                    continue

                if not isinstance(payload, dict):
                    logger.error(
                        "Schema event at partition=%s offset=%s is not an object; routing to DLQ",
                        message.partition,
                        message.offset,
                    )
                    write_to_dlq(payload, reason="schema event must be a JSON object")
                    consumer.commit()
                    continue

                dataset_name = payload.get("dataset_name", "unknown_dataset")
                schema = payload.get("schema")
                if not isinstance(schema, dict) or not schema:
                    logger.warning(
                        "Schema event at partition=%s offset=%s has no schema object; routing to DLQ",
                        message.partition,
                        message.offset,
                    )
                    write_to_dlq(
                        payload,
                        reason="schema event must contain a non-empty schema object",
                    )
                    consumer.commit()
                    continue

                try:
                    persisted = persist_with_retry(
                        {"dataset_name": dataset_name, "schema": schema},
                        lambda event: upsert_catalog_entry(
                            event["dataset_name"], event["schema"]
                        ),
                    )
                except Exception:
                    logger.exception(
                        "Failed to persist or dead-letter schema event for %s at "
                        "partition=%s offset=%s; leaving offset uncommitted",
                        dataset_name,
                        message.partition,
                        message.offset,
                    )
                    raise
                if not persisted:
                    logger.error(
                        "Schema event for %s at partition=%s offset=%s was routed to DLQ",
                        dataset_name,
                        message.partition,
                        message.offset,
                    )
                consumer.commit()
        except Exception:
            logger.exception("AI Catalog Kafka consumer failed; reconnecting")
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    logger.exception("Failed to close AI Catalog Kafka consumer")

        if not stop_event.is_set():
            stop_event.wait(CONSUMER_RECONNECT_SECONDS)


@app.get("/health")
def health():
    consumer_alive = _consumer_thread is not None and _consumer_thread.is_alive()
    try:
        with get_db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ai_catalog_dlq")
            dlq_count = int(cur.fetchone()[0])
    except Exception as exc:
        logger.exception("Failed to read AI Catalog DLQ count")
        raise HTTPException(
            status_code=503,
            detail="Unable to read AI Catalog dead-letter queue count",
        ) from exc
    return {
        "status": "ok" if consumer_alive else "degraded",
        "consumer_alive": consumer_alive,
        "schema_topic": SCHEMA_TOPIC,
        "lineage_topic": LINEAGE_TOPIC,
        "dlq_count": dlq_count,
    }


@app.get("/catalog")
def list_catalog():
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT dataset_name, ai_description, ai_tags, lineage_upstream,
                   lineage_downstream, last_updated_at
            FROM data_catalog_entries
            ORDER BY last_updated_at DESC
            LIMIT 100
            """
        )
        rows = cur.fetchall()
    return [
        {
            "dataset_name": r[0],
            "description": r[1],
            "tags": r[2],
            "lineage_upstream": r[3] or [],
            "lineage_downstream": r[4] or [],
            "updated_at": r[5].isoformat(),
        }
        for r in rows
    ]
