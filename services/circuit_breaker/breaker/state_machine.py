"""
breaker/state_machine.py

The circuit breaker itself: a pure state machine with no I/O, deliberately
separated from Kafka/Postgres so it can be tested exhaustively without any
infrastructure — same reasoning as manual_drift.py in Week 3.

Follows the classic three-state pattern (Nygard, "Release It!", 2007;
popularized by Martin Fowler) as implemented in Netflix Hystrix and
Resilience4j:

    CLOSED --(failure_threshold consecutive incidents)--> OPEN
    OPEN --(cooldown elapses)--> HALF_OPEN
    HALF_OPEN --(remediation succeeds)--> CLOSED
    HALF_OPEN --(remediation fails)--> OPEN  (cooldown grows via backoff)

Two changes from a textbook HTTP circuit breaker, both intentional:

1. "Requests" here are drift/anomaly incidents, not outbound calls the
   breaker itself makes — the thing being protected is whatever
   remediation actor consumes `remediation-requests` downstream (the
   Week 4 RCA Copilot), not this service.
2. Cooldown uses *equal-jitter* exponential backoff — half the computed
   backoff is guaranteed, the other half is randomized:
   equal_jitter = capped/2 + random(0, capped/2), plus a small additional
   random(0, cooldown_jitter_seconds) on top for extra decorrelation
   across monitors. This is deliberately NOT AWS's "full jitter"
   (random(0, capped)): full jitter is designed for client retries, where
   drawing a near-zero wait is harmless. For a circuit breaker cooldown,
   the whole point is to give the system real breathing room before
   probing again — a near-zero draw would let a trial straight back
   through right after tripping and defeat the mechanism. Equal jitter
   keeps the anti-thundering-herd property while guaranteeing a floor.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from breaker.config import BreakerTuning


class State(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class Action(str, Enum):
    FORWARD = "forward_to_remediation"   # allow through to remediation-requests
    BLOCK = "block_to_dlq"               # shunt to remediation-dlq + alert


@dataclass(frozen=True)
class BreakerState:
    monitor_name: str
    state: State = State.CLOSED
    consecutive_failures: int = 0
    trip_count: int = 0                       # total times this monitor has gone OPEN — drives backoff growth
    opened_at: Optional[datetime] = None
    cooldown_seconds: float = 0.0              # the (jittered) cooldown chosen for the current/last OPEN period
    half_open_trial_in_flight: bool = False    # HALF_OPEN allows exactly one trial at a time


@dataclass(frozen=True)
class Decision:
    new_state: BreakerState
    action: Action
    reason: str


def _compute_cooldown(tuning: BreakerTuning, trip_count: int) -> float:
    """
    Equal-jitter exponential backoff: guarantees at least half the computed
    backoff (unlike full jitter, which could draw near-zero and let a
    trial straight back through right after tripping), plus a small extra
    random component on top for decorrelation across different monitors'
    breakers. Actual sleep can therefore slightly exceed cooldown_max_seconds
    by up to cooldown_jitter_seconds — the max caps exponential growth of
    the base, not the final jittered value.
    """
    raw = tuning.cooldown_seconds * (tuning.cooldown_backoff_multiplier ** max(trip_count - 1, 0))
    capped = min(raw, tuning.cooldown_max_seconds)
    equal_jitter = (capped / 2) + random.uniform(0, capped / 2)
    return equal_jitter + random.uniform(0, tuning.cooldown_jitter_seconds)


def record_incident(state: BreakerState, tuning: BreakerTuning, now: datetime) -> Decision:
    """Call this when a new drift/anomaly incident arrives for `state.monitor_name`."""

    if state.state == State.CLOSED:
        failures = state.consecutive_failures + 1
        if failures >= tuning.failure_threshold:
            trip_count = state.trip_count + 1
            cooldown = _compute_cooldown(tuning, trip_count)
            new_state = replace(
                state, state=State.OPEN, consecutive_failures=failures,
                trip_count=trip_count, opened_at=now, cooldown_seconds=cooldown,
                half_open_trial_in_flight=False,
            )
            return Decision(
                new_state=new_state, action=Action.BLOCK,
                reason=f"{failures} consecutive incidents >= threshold {tuning.failure_threshold} — tripping OPEN for ~{cooldown:.0f}s",
            )
        new_state = replace(state, consecutive_failures=failures)
        return Decision(
            new_state=new_state, action=Action.FORWARD,
            reason=f"CLOSED, {failures}/{tuning.failure_threshold} consecutive incidents — forwarding",
        )

    if state.state == State.OPEN:
        elapsed = (now - state.opened_at).total_seconds() if state.opened_at else float("inf")
        if elapsed >= state.cooldown_seconds:
            # Cooldown elapsed: allow exactly one trial through as HALF_OPEN.
            new_state = replace(state, state=State.HALF_OPEN, half_open_trial_in_flight=True)
            return Decision(
                new_state=new_state, action=Action.FORWARD,
                reason=f"Cooldown elapsed ({elapsed:.0f}s >= {state.cooldown_seconds:.0f}s) — allowing one HALF_OPEN trial",
            )
        # A blocked incident is a *rejected call*, not a new failure (Resilience4j's
        # CallNotPermittedException is likewise not recorded as a failure). Counting
        # them here made consecutive_failures balloon (16 observed on 2026-10-05)
        # and carried no information beyond trip_count/opened_at.
        return Decision(
            new_state=state, action=Action.BLOCK,
            reason=f"OPEN, cooldown not yet elapsed ({elapsed:.0f}s / {state.cooldown_seconds:.0f}s) — blocked to DLQ",
        )

    # HALF_OPEN
    if state.half_open_trial_in_flight:
        return Decision(
            new_state=state, action=Action.BLOCK,
            reason="HALF_OPEN trial already in flight — additional incidents blocked until outcome is reported",
        )
    # Defensive: shouldn't normally reach HALF_OPEN with no trial in flight, but
    # if it does (e.g. after a restart with stale persisted state), treat it as
    # OPEN behavior — safer to under-trust an unknown HALF_OPEN than to let two
    # trials race against a downstream remediation actor that isn't idempotent.
    new_state = replace(state, half_open_trial_in_flight=True)
    return Decision(
        new_state=new_state, action=Action.FORWARD,
        reason="HALF_OPEN with no trial recorded (likely post-restart) — allowing one trial",
    )


def record_outcome(state: BreakerState, tuning: BreakerTuning, success: bool, now: datetime) -> BreakerState:
    """
    Call this when the remediation actor (RCA Copilot, once built) reports
    whether its attempt succeeded. Only meaningful from HALF_OPEN; a report
    arriving while CLOSED (e.g. a stale/duplicate callback) is a no-op aside
    from logging, since CLOSED already means "healthy."
    """
    if state.state != State.HALF_OPEN:
        return state

    if success:
        return replace(
            state, state=State.CLOSED, consecutive_failures=0,
            trip_count=0, opened_at=None, cooldown_seconds=0.0,
            half_open_trial_in_flight=False,
        )

    trip_count = state.trip_count + 1
    cooldown = _compute_cooldown(tuning, trip_count)
    return replace(
        state, state=State.OPEN, trip_count=trip_count, opened_at=now,
        cooldown_seconds=cooldown, half_open_trial_in_flight=False,
    )
