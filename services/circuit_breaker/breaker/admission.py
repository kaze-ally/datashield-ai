"""
breaker/admission.py

Pure, I/O-free guards that sit *in front of* the state machine so a flood of
drift events cannot be mistaken for a flood of independent incidents.

Why this exists (2026-10-05 live test): the breaker was restarted after being
stopped for days. Its consumer group resumed from the last committed offset and
replayed every drift event published in the meantime (Airflow runs two hourly
/check calls, and the drift monitor re-published on every check while drift
persisted). The state machine counted each as a fresh incident, tripped
immediately, then wrote one audit row + one remediation-dlq message per
replayed event.

Three guards, all deterministic and unit-tested:

1. Staleness  - an event older than max_event_age_seconds describes a world that
                no longer exists; remediating it is meaningless. Skip it.
2. Duplicates - Kafka is at-least-once; the same report_id must count once.
3. Throttling - once OPEN, "still blocked" is not news. Emit the audit row /
                DLQ message on the state transition and then at most once per
                interval (alert de-duplication: notify on state *change*, not on
                every repeat of the same condition).
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple


def parse_event_time(raw: object) -> Optional[datetime]:
    """Parse an ISO-8601 string into an aware UTC datetime; None if absent/unparseable.
    Naive timestamps (the drift monitor emits utcnow().isoformat()) are treated as UTC."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class EventAdmission:
    def __init__(self, max_age_seconds: float, recent_ids: int = 2048) -> None:
        self.max_age_seconds = max_age_seconds
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self._cap = recent_ids

    def check(self, report_id: Optional[str], detected_at: object, now: datetime) -> Tuple[bool, str]:
        """Return (admit, reason). reason is 'ok', 'stale' or 'duplicate'."""
        ts = parse_event_time(detected_at)
        if self.max_age_seconds > 0 and ts is not None:
            if (now - ts).total_seconds() > self.max_age_seconds:
                return False, "stale"
        if report_id:
            if report_id in self._seen:
                return False, "duplicate"
            self._seen[report_id] = None
            while len(self._seen) > self._cap:
                self._seen.popitem(last=False)
        return True, "ok"


class BlockThrottle:
    """Emit-on-transition, then at most once per interval per monitor."""

    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = interval_seconds
        self._last: Dict[str, float] = {}

    def should_emit(self, monitor: str, now_monotonic: float, is_transition: bool) -> bool:
        last = self._last.get(monitor)
        if is_transition or last is None or (now_monotonic - last) >= self.interval_seconds:
            self._last[monitor] = now_monotonic
            return True
        return False
