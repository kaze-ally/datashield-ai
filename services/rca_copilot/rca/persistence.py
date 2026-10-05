"""
rca/persistence.py

Persists each RCA run to Postgres — same defensive-casting discipline as
Weeks 3/4: confidence could arrive as a numpy float if this pipeline ever
gets fed from a pandas-derived source, so cast explicitly rather than
trusting psycopg2's default adapters.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Optional

import psycopg2


def _to_native(value: Any) -> Any:
    if hasattr(value, "item") and not isinstance(value, (dict, list, str, bytes)):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    return value


def persist_rca_report(
    dsn: str,
    table: str,
    *,
    monitor_name: str,
    report_id: Optional[str],
    root_cause_category: str,
    narrative: str,
    severity: str,
    confidence: float,
    model_used: str,
    outcome_success: bool,
    outcome_reason: str,
    context_snapshot: dict,
    created_at: datetime,
) -> str:
    rca_id = str(uuid.uuid4())
    query = f"""
        INSERT INTO {table} (
            rca_id, monitor_name, report_id, root_cause_category, narrative,
            severity, confidence, model_used, outcome_success, outcome_reason,
            context_snapshot, created_at
        ) VALUES (
            %(rca_id)s, %(monitor_name)s, %(report_id)s, %(root_cause_category)s, %(narrative)s,
            %(severity)s, %(confidence)s, %(model_used)s, %(outcome_success)s, %(outcome_reason)s,
            %(context_snapshot)s, %(created_at)s
        )
    """
    payload = {
        "rca_id": rca_id, "monitor_name": monitor_name, "report_id": report_id,
        "root_cause_category": root_cause_category, "narrative": narrative,
        "severity": severity, "confidence": _to_native(confidence), "model_used": model_used,
        "outcome_success": bool(outcome_success), "outcome_reason": outcome_reason,
        "context_snapshot": json.dumps(_to_native(context_snapshot), default=str),
        "created_at": created_at,
    }
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)
        conn.commit()
    return rca_id
