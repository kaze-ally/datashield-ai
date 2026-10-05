"""
drift_detector/persistence.py

Writes drift-check results to Postgres. Every value that originates from
numpy/pandas/scipy is explicitly cast to a native Python type before it
reaches psycopg2. This project already lost time once to a numpy.bool_ ->
psycopg2 type-adaptation failure in the Isolation Forest persistence path
(detection succeeded, persistence failed silently). PSI scores are
numpy.float64 and drift flags can easily end up as numpy.bool_ here too,
so we don't rely on psycopg2's default adapters for anything Evidently or
scipy hands back.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2

logger = logging.getLogger(__name__)


def _to_native(value: Any) -> Any:
    """Recursively convert numpy/pandas scalar types to native Python types."""
    if hasattr(value, "item") and not isinstance(value, (dict, list, str, bytes)):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    if isinstance(value, dict):
        return {k: _to_native(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_native(v) for v in value]
    return value


def persist_drift_report(
    dsn: str,
    table: str,
    *,
    monitor_name: str,
    run_timestamp: datetime,
    reference_start: datetime,
    reference_end: datetime,
    current_start: datetime,
    current_end: datetime,
    dataset_drift_detected: bool,
    drift_share: float,
    column_results: Dict[str, Any],
    evidently_raw: Optional[Dict[str, Any]],
    html_report_path: Optional[str],
) -> str:
    """Inserts one row per drift-check run. Returns the generated report_id."""
    report_id = str(uuid.uuid4())

    payload = {
        "report_id": report_id,
        "monitor_name": monitor_name,
        "run_timestamp": run_timestamp,
        "reference_start": reference_start,
        "reference_end": reference_end,
        "current_start": current_start,
        "current_end": current_end,
        "dataset_drift_detected": bool(dataset_drift_detected),
        "drift_share": float(drift_share),
        "column_results": json.dumps(_to_native(column_results)),
        "evidently_raw": json.dumps(_to_native(evidently_raw)) if evidently_raw is not None else None,
        "html_report_path": html_report_path,
    }

    query = f"""
        INSERT INTO {table} (
            report_id, monitor_name, run_timestamp,
            reference_start, reference_end, current_start, current_end,
            dataset_drift_detected, drift_share,
            column_results, evidently_raw, html_report_path
        ) VALUES (
            %(report_id)s, %(monitor_name)s, %(run_timestamp)s,
            %(reference_start)s, %(reference_end)s, %(current_start)s, %(current_end)s,
            %(dataset_drift_detected)s, %(drift_share)s,
            %(column_results)s, %(evidently_raw)s, %(html_report_path)s
        )
    """

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)
        conn.commit()

    logger.info(
        "Persisted drift report %s for %s (dataset_drift=%s, drift_share=%.3f)",
        report_id, monitor_name, dataset_drift_detected, drift_share,
    )
    return report_id
