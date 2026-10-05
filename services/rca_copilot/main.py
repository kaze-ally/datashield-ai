"""
services/rca_copilot/main.py

RCA Copilot service — Week 4 part 2. Same shape as circuit_breaker: FastAPI
for health/inspection, background thread for the Kafka consumer loop.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

import psycopg2
import psycopg2.extras

from rca.config import load_rca_config
from rca.kafka_consumer import run_consumer_loop

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rca_copilot")

CONFIG_PATH = os.environ.get("RCA_CONFIG_PATH", "/app/config/rca_copilot_config.yaml")
POSTGRES_DSN = os.environ.get("DATASHIELD_POSTGRES_DSN", "")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

config = load_rca_config(CONFIG_PATH)
app = FastAPI(title="DataShield AI — RCA Copilot", version="0.1.0")

_stop_event = threading.Event()
_consumer_thread: Optional[threading.Thread] = None


@app.on_event("startup")
def _start_consumer() -> None:
    global _consumer_thread
    _consumer_thread = threading.Thread(
        target=run_consumer_loop,
        args=(_stop_event, POSTGRES_DSN, KAFKA_BOOTSTRAP_SERVERS, config),
        daemon=True, name="rca-copilot-consumer",
    )
    _consumer_thread.start()


@app.on_event("shutdown")
def _stop_consumer() -> None:
    _stop_event.set()
    if _consumer_thread is not None:
        _consumer_thread.join(timeout=10)


class RCAReportItem(BaseModel):
    rca_id: str
    monitor_name: str
    report_id: Optional[str]
    root_cause_category: str
    narrative: str
    severity: str
    confidence: float
    model_used: str
    outcome_success: bool
    outcome_reason: str
    created_at: datetime


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "consumer_alive": _consumer_thread.is_alive() if _consumer_thread else False,
        "source_topic": config.source.topic,
    }


@app.get("/reports/{monitor_name}", response_model=List[RCAReportItem])
def get_reports(monitor_name: str, limit: int = 10) -> List[RCAReportItem]:
    query = f"""
        SELECT rca_id, monitor_name, report_id, root_cause_category, narrative,
               severity, confidence, model_used, outcome_success, outcome_reason, created_at
        FROM {config.outputs.postgres_table}
        WHERE monitor_name = %(monitor_name)s
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """
    with psycopg2.connect(POSTGRES_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, {"monitor_name": monitor_name, "limit": limit})
            rows = cur.fetchall()
    return [RCAReportItem(**dict(row)) for row in rows]
