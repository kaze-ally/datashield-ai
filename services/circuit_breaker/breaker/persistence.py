"""
breaker/persistence.py

Persists breaker state (one row per monitor_name, upserted) and a
transition audit log (append-only) to Postgres. Same defensive-casting
discipline as Week 3's drift_reports persistence — Decision.reason strings
are plain Python already, but consecutive_failures/trip_count could arrive
as numpy ints if this ever gets fed from a pandas-derived pipeline, so we
don't assume otherwise.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Optional

import psycopg2

from breaker.state_machine import BreakerState, State

logger = logging.getLogger(__name__)


def _to_native(value: Any) -> Any:
    if hasattr(value, "item") and not isinstance(value, (dict, list, str, bytes)):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    return value


def load_state(dsn: str, table: str, monitor_name: str) -> Optional[BreakerState]:
    """Returns the persisted state for a monitor, or None if it's never been seen."""
    query = f"""
        SELECT state, consecutive_failures, trip_count, opened_at,
               cooldown_seconds, half_open_trial_in_flight
        FROM {table}
        WHERE monitor_name = %(monitor_name)s
    """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"monitor_name": monitor_name})
            row = cur.fetchone()

    if row is None:
        return None

    state, consecutive_failures, trip_count, opened_at, cooldown_seconds, half_open_trial_in_flight = row
    return BreakerState(
        monitor_name=monitor_name,
        state=State(state),
        consecutive_failures=consecutive_failures,
        trip_count=trip_count,
        opened_at=opened_at,
        cooldown_seconds=cooldown_seconds,
        half_open_trial_in_flight=half_open_trial_in_flight,
    )


def save_state(dsn: str, table: str, state: BreakerState) -> None:
    """Upserts the current state for state.monitor_name."""
    query = f"""
        INSERT INTO {table} (
            monitor_name, state, consecutive_failures, trip_count,
            opened_at, cooldown_seconds, half_open_trial_in_flight, updated_at
        ) VALUES (
            %(monitor_name)s, %(state)s, %(consecutive_failures)s, %(trip_count)s,
            %(opened_at)s, %(cooldown_seconds)s, %(half_open_trial_in_flight)s, now()
        )
        ON CONFLICT (monitor_name) DO UPDATE SET
            state = EXCLUDED.state,
            consecutive_failures = EXCLUDED.consecutive_failures,
            trip_count = EXCLUDED.trip_count,
            opened_at = EXCLUDED.opened_at,
            cooldown_seconds = EXCLUDED.cooldown_seconds,
            half_open_trial_in_flight = EXCLUDED.half_open_trial_in_flight,
            updated_at = now()
    """
    payload = {
        "monitor_name": state.monitor_name,
        "state": state.state.value,
        "consecutive_failures": _to_native(state.consecutive_failures),
        "trip_count": _to_native(state.trip_count),
        "opened_at": state.opened_at,
        "cooldown_seconds": _to_native(state.cooldown_seconds),
        "half_open_trial_in_flight": bool(state.half_open_trial_in_flight),
    }
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)
        conn.commit()


def log_event(
    dsn: str,
    table: str,
    *,
    monitor_name: str,
    event_type: str,
    from_state: Optional[str],
    to_state: str,
    reason: str,
    related_report_id: Optional[str],
) -> str:
    """Appends one row to the audit log. Returns the generated event_id."""
    event_id = str(uuid.uuid4())
    query = f"""
        INSERT INTO {table} (
            event_id, monitor_name, event_type, from_state, to_state,
            reason, related_report_id, created_at
        ) VALUES (
            %(event_id)s, %(monitor_name)s, %(event_type)s, %(from_state)s, %(to_state)s,
            %(reason)s, %(related_report_id)s, now()
        )
    """
    payload = {
        "event_id": event_id, "monitor_name": monitor_name, "event_type": event_type,
        "from_state": from_state, "to_state": to_state, "reason": reason,
        "related_report_id": related_report_id,
    }
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, payload)
        conn.commit()

    logger.info("Breaker event [%s] %s: %s -> %s (%s)", monitor_name, event_type, from_state, to_state, reason)
    return event_id


def fetch_recent_events(dsn: str, table: str, monitor_name: str, limit: int = 20) -> list[dict]:
    query = f"""
        SELECT event_id, event_type, from_state, to_state, reason, related_report_id, created_at
        FROM {table}
        WHERE monitor_name = %(monitor_name)s
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"monitor_name": monitor_name, "limit": limit})
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]
