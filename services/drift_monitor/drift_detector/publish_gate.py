"""
drift_detector/publish_gate.py

Edge-triggered publishing for drift-events, with a re-notify heartbeat.

Problem (observed 2026-10-05): /check was *level-triggered* - every call that
found drift published a new drift-events message with a fresh report_id. Two
hourly Airflow DAGs (batch_drift_check and orders_validation_pipeline) both call
/check, plus manual calls, so one continuing drift episode produced a steady
stream of "new" incidents. Downstream (circuit breaker) cannot tell a repeat of
the same condition from a new one, so it tripped on noise.

Fix: treat an episode of drift as ONE incident.
  * publish on the clean -> drifted edge,
  * while drift persists, re-announce at most every `renotify_seconds`
    (a heartbeat so a long-running episode is not forgotten),
  * never publish twice within `min_publish_interval_seconds` (flap damping /
    hysteresis: clean->drift->clean->drift in quick succession is one episode),
  * `force=True` bypasses the gate for demos/tests (POST /check?force_publish=true).

Every check is still persisted to Postgres and shown in reports; only the
Kafka announcement is gated.

Note on state: kept in memory (single-instance service). After a restart the first
drifted check is treated as an edge and publishes once - a deliberate, safe
fail-open: it can duplicate one event, never swallow one.
"""
from __future__ import annotations

import datetime as dt
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class _MonitorState:
    last_drifted: bool = False
    last_published_at: Optional[dt.datetime] = None


class DriftPublishGate:
    def __init__(self, renotify_seconds: float = 1800.0, min_publish_interval_seconds: float = 120.0) -> None:
        self.renotify_seconds = renotify_seconds
        self.min_publish_interval_seconds = min_publish_interval_seconds
        self._states: Dict[str, _MonitorState] = {}
        self._lock = threading.Lock()

    def decide(self, monitor: str, drifted: bool, now: dt.datetime, force: bool = False) -> Tuple[bool, str]:
        """Return (publish, reason). Records the decision; call exactly once per check."""
        with self._lock:
            st = self._states.setdefault(monitor, _MonitorState())
            was_drifted = st.last_drifted
            st.last_drifted = drifted

            if not drifted:
                return False, "no drift"

            since = (now - st.last_published_at).total_seconds() if st.last_published_at else None

            if force:
                st.last_published_at = now
                return True, "forced"
            if since is not None and since < self.min_publish_interval_seconds:
                return False, f"damped: last event {since:.0f}s ago (< {self.min_publish_interval_seconds:.0f}s)"
            if not was_drifted:
                st.last_published_at = now
                return True, "new drift episode"
            if since is None or since >= self.renotify_seconds:
                st.last_published_at = now
                return True, "drift persists - re-notify heartbeat"
            return False, f"suppressed: same drift episode, last event {since:.0f}s ago (< {self.renotify_seconds:.0f}s)"

    def forget_publish(self, monitor: str) -> None:
        """Call if the Kafka publish failed, so the next check may retry instead of being suppressed."""
        with self._lock:
            st = self._states.get(monitor)
            if st:
                st.last_published_at = None
