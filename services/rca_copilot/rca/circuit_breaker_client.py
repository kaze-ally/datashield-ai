"""
rca/circuit_breaker_client.py

Calls circuit_breaker's /report-outcome — this is the actual integration
point between RCA Copilot and the Circuit Breaker. A failure here must be
loud: if the breaker never hears back, a HALF_OPEN trial sits stuck
without a report_outcome, meaning the breaker will never advance for that
monitor until the next incident retries a trial. Not silently swallowed.
"""
from __future__ import annotations

import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def report_outcome(circuit_breaker_url: str, monitor_name: str, report_id: Optional[str], success: bool, timeout: float = 10.0) -> bool:
    """Returns True if the breaker acknowledged the report, False otherwise (never raises)."""
    try:
        resp = requests.post(
            f"{circuit_breaker_url}/report-outcome",
            json={"monitor_name": monitor_name, "report_id": report_id, "success": success},
            timeout=timeout,
        )
        resp.raise_for_status()
        logger.info("Reported outcome to circuit breaker for %s: success=%s -> %s", monitor_name, success, resp.json())
        return True
    except Exception:
        logger.exception(
            "Failed to report outcome to circuit breaker for monitor=%s (success=%s) — "
            "the breaker's HALF_OPEN trial for this monitor may remain stuck until the next incident.",
            monitor_name, success,
        )
        return False
