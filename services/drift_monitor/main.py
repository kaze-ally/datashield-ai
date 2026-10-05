"""
services/drift_monitor/main.py

Drift Monitor service — Week 3.

A small FastAPI service, following the same shape as sidecar_validator,
llm_gateway, and ai_catalog: its own Dockerfile / main.py / requirements.txt.
batch_drift_check_dag.py triggers this on a schedule via a single HTTP call;
all the actual work (pulling reference/current windows, running the drift
checks, persisting results, publishing to Kafka) happens inside this
service, not inside the DAG.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import re
from typing import Optional

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from drift_detector.config import load_drift_config
from drift_detector.data_loader import load_reference_and_current
from drift_detector.evidently_runner import run_evidently_report
from drift_detector.kafka_publisher import publish_drift_event
from drift_detector.manual_drift import compute_manual_drift_verdict
from drift_detector.persistence import persist_drift_report
from drift_detector.publish_gate import DriftPublishGate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("drift_monitor")

CONFIG_PATH = os.environ.get("DRIFT_CONFIG_PATH", "/app/config/drift_detection_config.yaml")
POSTGRES_DSN = os.environ.get("DATASHIELD_POSTGRES_DSN", "")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

app = FastAPI(title="DataShield AI — Drift Monitor", version="0.1.0")
config = load_drift_config(CONFIG_PATH)
publish_gate = DriftPublishGate(
    renotify_seconds=config.alerting.renotify_minutes * 60,
    min_publish_interval_seconds=config.alerting.min_publish_interval_seconds,
)


class DriftCheckResponse(BaseModel):
    report_id: str
    monitor_name: str
    dataset_drift_detected: bool
    drift_share: float
    html_report_path: Optional[str] = None
    event_published: bool = False
    publish_reason: str = ""


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "monitor_name": config.monitor_name}


@app.get("/reports")
def recent_reports(limit: int = Query(default=5, ge=1, le=50)) -> list[dict]:
    """Return the latest persisted checks for the configured monitor."""
    table = config.outputs.postgres_table
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise HTTPException(status_code=500, detail="Invalid drift report table configuration")
    try:
        with psycopg2.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                f"""SELECT report_id, run_timestamp, dataset_drift_detected,
                           drift_share, column_results, html_report_path
                    FROM {table}
                    WHERE monitor_name = %s
                    ORDER BY run_timestamp DESC
                    LIMIT %s""",
                (config.monitor_name, limit),
            )
            rows = cur.fetchall()
    except Exception as exc:
        logger.exception("Could not load recent drift reports")
        raise HTTPException(status_code=503, detail="Drift report store unavailable") from exc
    return [
        {
            "report_id": str(row[0]),
            "run_timestamp": row[1].isoformat() if row[1] else None,
            "dataset_drift_detected": bool(row[2]),
            "drift_share": float(row[3]),
            "column_results": row[4],
            "html_report_path": row[5],
        }
        for row in rows
    ]


@app.post("/check", response_model=DriftCheckResponse)
def run_check(force_publish: bool = False) -> DriftCheckResponse:
    now = dt.datetime.utcnow()
    try:
        reference_df, current_df, bounds = load_reference_and_current(POSTGRES_DSN, config, now=now)
    except ValueError as exc:
        # Undersized window etc. — a 422 so Airflow's task fails loudly and
        # retries, instead of silently recording "no drift".
        logger.error("Drift check aborted: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    verdict = compute_manual_drift_verdict(reference_df, current_df, config)

    evidently_dict, html_path = run_evidently_report(
        current_df=current_df,
        reference_df=reference_df,
        method=config.drift_detection.method,
        html_output_dir=config.outputs.html_report_dir,
        report_filename=f"{config.monitor_name}_{now.strftime('%Y%m%dT%H%M%S')}.html",
    )

    report_id = persist_drift_report(
        POSTGRES_DSN,
        config.outputs.postgres_table,
        monitor_name=config.monitor_name,
        run_timestamp=now,
        reference_start=bounds.reference_start,
        reference_end=bounds.reference_end,
        current_start=bounds.current_start,
        current_end=bounds.current_end,
        dataset_drift_detected=verdict.dataset_drift,
        drift_share=verdict.drift_share,
        column_results=verdict.as_dict(),
        evidently_raw=evidently_dict,
        html_report_path=html_path,
    )

    publish, publish_reason = publish_gate.decide(
        config.monitor_name, verdict.dataset_drift, now, force=force_publish,
    )
    published = False
    if publish:
        try:
            publish_drift_event(
                KAFKA_BOOTSTRAP_SERVERS,
                config.outputs.kafka_drift_topic,
                {
                    "event_type": "dataset_drift_detected",
                    "monitor_name": config.monitor_name,
                    "report_id": report_id,
                    "drift_share": verdict.drift_share,
                    "detected_at": now.isoformat(),
                    "publish_reason": publish_reason,
                },
            )
            published = True
        except Exception:
            # A publish failure shouldn't erase a result that's already
            # safely persisted in Postgres - log loudly, don't lose the check,
            # and let the next check retry instead of being gated out.
            publish_gate.forget_publish(config.monitor_name)
            logger.exception("Failed to publish drift-events message for report %s", report_id)
    elif verdict.dataset_drift:
        logger.info("Drift persisted (report %s) but event suppressed: %s", report_id, publish_reason)

    return DriftCheckResponse(
        report_id=report_id,
        monitor_name=config.monitor_name,
        dataset_drift_detected=verdict.dataset_drift,
        drift_share=verdict.drift_share,
        html_report_path=html_path,
        event_published=published,
        publish_reason=publish_reason,
    )
