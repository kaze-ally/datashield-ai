"""
services/circuit_breaker/main.py

Remediation Circuit Breaker service — Week 4.

Same shape as drift_monitor: FastAPI for the synchronous API surface,
plus here a background daemon thread running the Kafka consumer loop
against drift-events (and future incident topics). The HTTP side exposes
health, current breaker state per monitor, and the /report-outcome
endpoint that the not-yet-built RCA Copilot will call once a remediation
attempt (forwarded via remediation-requests) finishes.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from breaker.config import load_breaker_config
from breaker.kafka_consumer import STATS, run_consumer_loop
from breaker.persistence import fetch_recent_events, load_state, log_event, save_state
from breaker.state_machine import BreakerState, record_outcome

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("circuit_breaker")

CONFIG_PATH = os.environ.get("BREAKER_CONFIG_PATH", "/app/config/circuit_breaker_config.yaml")
POSTGRES_DSN = os.environ.get("DATASHIELD_POSTGRES_DSN", "")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

config = load_breaker_config(CONFIG_PATH)
app = FastAPI(title="DataShield AI — Remediation Circuit Breaker", version="0.1.0")

_stop_event = threading.Event()
_consumer_thread: Optional[threading.Thread] = None


@app.on_event("startup")
def _start_consumer() -> None:
    global _consumer_thread
    _consumer_thread = threading.Thread(
        target=run_consumer_loop,
        args=(_stop_event, POSTGRES_DSN, KAFKA_BOOTSTRAP_SERVERS, config),
        daemon=True,
        name="circuit-breaker-consumer",
    )
    _consumer_thread.start()
    logger.info("Circuit breaker consumer thread started.")


@app.on_event("shutdown")
def _stop_consumer() -> None:
    _stop_event.set()
    if _consumer_thread is not None:
        _consumer_thread.join(timeout=10)


class StateResponse(BaseModel):
    monitor_name: str
    state: str
    consecutive_failures: int
    trip_count: int
    cooldown_seconds: float
    cooldown_elapsed: bool = False


class OutcomeRequest(BaseModel):
    monitor_name: str
    report_id: Optional[str] = None
    success: bool


class OutcomeResponse(BaseModel):
    monitor_name: str
    state: str


class EventItem(BaseModel):
    event_id: str
    event_type: str
    from_state: Optional[str]
    to_state: str
    reason: str
    related_report_id: Optional[str]
    created_at: datetime


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "consumer_alive": _consumer_thread.is_alive() if _consumer_thread else False,
        "source_topics": [s.topic for s in config.sources],
        "event_stats": dict(STATS),
        "max_event_age_seconds": config.breaker.max_event_age_seconds,
    }


@app.get("/state/{monitor_name}", response_model=StateResponse)
def get_state(monitor_name: str) -> StateResponse:
    state = load_state(POSTGRES_DSN, config.outputs.postgres_state_table, monitor_name)
    if state is None:
        # No incidents recorded yet for this monitor — that's CLOSED by definition, not a 404.
        state = BreakerState(monitor_name=monitor_name)
    cooldown_elapsed = bool(
        state.state.value == "OPEN"
        and state.opened_at
        and (datetime.now(timezone.utc) - state.opened_at).total_seconds() >= state.cooldown_seconds
    )
    return StateResponse(
        monitor_name=state.monitor_name,
        state=state.state.value,
        consecutive_failures=state.consecutive_failures,
        trip_count=state.trip_count,
        cooldown_seconds=state.cooldown_seconds,
        cooldown_elapsed=cooldown_elapsed,
    )


@app.post("/report-outcome", response_model=OutcomeResponse)
def report_outcome(body: OutcomeRequest) -> OutcomeResponse:
    """
    Called by a remediation actor (the Week 4 RCA Copilot, once built) after
    attempting whatever remediation-requests asked it to do. Advances
    HALF_OPEN -> CLOSED on success, or HALF_OPEN -> OPEN (with grown
    backoff) on failure. A report for a monitor that isn't currently
    HALF_OPEN is accepted but is a no-op on the state machine — most likely
    a stale/duplicate callback, not an error worth failing the caller for.
    """
    existing = load_state(POSTGRES_DSN, config.outputs.postgres_state_table, body.monitor_name)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"No breaker state exists yet for monitor '{body.monitor_name}'")

    from_state = existing.state.value
    now = datetime.now(timezone.utc)
    new_state = record_outcome(existing, config.breaker, body.success, now)

    if new_state.state.value != from_state:
        save_state(POSTGRES_DSN, config.outputs.postgres_state_table, new_state)
        log_event(
            POSTGRES_DSN, config.outputs.postgres_events_table,
            monitor_name=body.monitor_name,
            event_type="outcome_success" if body.success else "outcome_failure",
            from_state=from_state, to_state=new_state.state.value,
            reason=f"Remediation outcome reported: success={body.success}",
            related_report_id=body.report_id,
        )

    return OutcomeResponse(monitor_name=body.monitor_name, state=new_state.state.value)


@app.get("/events/{monitor_name}", response_model=List[EventItem])
def get_events(monitor_name: str, limit: int = 20) -> List[EventItem]:
    rows = fetch_recent_events(POSTGRES_DSN, config.outputs.postgres_events_table, monitor_name, limit)
    return [EventItem(**row) for row in rows]
