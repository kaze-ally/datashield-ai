"""
rca/context_loader.py

Gathers the diagnostic context an RCA narrative needs before calling the
LLM — matches Paper 6's "aggregates the critical runtime diagnostic
information" step, which the paper treats as separate from and prior to
the LLM call itself. Two sources:

1. The full drift_reports row for this report_id (per-column PSI/chi2
   scores, drift_share, window bounds) — the actual evidence.
2. This monitor's recent circuit_breaker event history — so the narrative
   can note "this is the 3rd trip this week" rather than treating every
   incident as if it's the first.

Both are best-effort: a context-gathering failure should degrade the
narrative's quality, not block RCA from running entirely. An LLM given a
report_id and no history is still more useful than skipping the whole
incident because the breaker's history endpoint was briefly unreachable.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
import requests

logger = logging.getLogger(__name__)


@dataclass
class Context:
    monitor_name: str
    report_id: Optional[str]
    drift_report: Optional[Dict[str, Any]] = None
    breaker_history: List[Dict[str, Any]] = field(default_factory=list)
    incident: Optional[Dict[str, Any]] = None


def _fetch_drift_report(dsn: str, table: str, report_id: str) -> Optional[Dict[str, Any]]:
    query = f"""
        SELECT monitor_name, run_timestamp, dataset_drift_detected, drift_share,
               column_results, reference_start, reference_end, current_start, current_end
        FROM {table}
        WHERE report_id = %(report_id)s
    """
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, {"report_id": report_id})
                row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        logger.exception("Failed to fetch drift_reports row for report_id=%s", report_id)
        return None


def _fetch_breaker_history(circuit_breaker_url: str, monitor_name: str, limit: int = 10) -> List[Dict[str, Any]]:
    try:
        resp = requests.get(f"{circuit_breaker_url}/events/{monitor_name}", params={"limit": limit}, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Could not fetch circuit breaker history for %s — proceeding without it", monitor_name)
        return []


def load_context(
    dsn: str, drift_reports_table: str, circuit_breaker_url: str,
    monitor_name: str, report_id: Optional[str], incident: Optional[Dict[str, Any]] = None,
) -> Context:
    drift_report = _fetch_drift_report(dsn, drift_reports_table, report_id) if report_id else None
    breaker_history = _fetch_breaker_history(circuit_breaker_url, monitor_name)
    return Context(
        monitor_name=monitor_name, report_id=report_id, drift_report=drift_report,
        breaker_history=breaker_history, incident=incident,
    )
