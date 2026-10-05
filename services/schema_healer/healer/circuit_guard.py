"""
services/schema_healer/healer/circuit_guard.py

Paper 7's point (autonomous remediation needs a circuit breaker so it can't
loop forever masking a real bug) applies here too, but not through the
existing `circuit_breaker` service: that service's Kafka/HTTP contract is
specifically wired to drift-events -> remediation-requests, one incident at
a time, with a human-reportable outcome. Schema mismatches arrive per
*message*, potentially hundreds/sec from one broken producer, and calling
an LLM for every single one of them during an upstream outage is exactly
the kind of runaway-cost/latency failure mode Paper 7 warns about — just on
the LLM-gateway axis instead of the "auto-remediation script" axis.

So this is a second, smaller instance of the same idea, scoped per
contract_name: after `failure_threshold` consecutive healing failures, stop
calling the LLM for that contract until a cooldown elapses, and DLQ
immediately with `healer_paused` instead. Reuses the equal-jitter backoff
fix from README_week4.md's Week 4 write-up (full jitter can draw near-zero
and let a trial straight back through right after tripping) rather than
re-discovering that bug a second time.

Pure logic, no I/O — same testing philosophy as
`circuit_breaker/breaker/state_machine.py`. `persistence.py` in this
service is the thin Postgres-backed wrapper that loads/saves
`CircuitGuardState` per contract_name; this module never touches a
database or a clock other than what's passed in.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class CircuitGuardConfig:
    failure_threshold: int = 3
    base_cooldown_seconds: float = 120.0
    backoff_multiplier: float = 2.0
    max_cooldown_seconds: float = 3600.0


@dataclass(frozen=True)
class CircuitGuardState:
    contract_name: str
    consecutive_failures: int = 0
    paused_until: datetime | None = None
    trip_count: int = 0


@dataclass(frozen=True)
class GuardDecision:
    allowed: bool
    reason: str


def _equal_jitter_seconds(ceiling: float) -> float:
    """Guarantees at least half of `ceiling`, unlike full jitter which can
    draw near zero. Same fix as circuit_breaker's Week 4 correction."""
    return ceiling / 2 + random.uniform(0, ceiling / 2)


def check(state: CircuitGuardState, now: datetime) -> GuardDecision:
    """Should the healer even attempt an LLM call for this contract right now?"""
    if state.paused_until is not None and now < state.paused_until:
        remaining = (state.paused_until - now).total_seconds()
        return GuardDecision(
            allowed=False,
            reason=f"healer_paused: {state.consecutive_failures} consecutive failures, "
            f"{remaining:.0f}s remaining in cooldown",
        )
    return GuardDecision(allowed=True, reason="ok")


def record_success(state: CircuitGuardState) -> CircuitGuardState:
    """A successful heal (mapping applied AND re-validation passed) resets
    the guard fully — mirrors circuit_breaker's HALF_OPEN -> CLOSED reset."""
    return replace(state, consecutive_failures=0, paused_until=None)


def record_failure(
    state: CircuitGuardState, now: datetime, config: CircuitGuardConfig
) -> CircuitGuardState:
    """A failed heal (LLM rejected, mapping rejected, or post-heal
    re-validation still fails) increments the counter. Once the threshold is
    crossed, pause this contract for an equal-jitter backoff window that
    grows with each additional trip, capped at max_cooldown_seconds."""
    new_failures = state.consecutive_failures + 1
    if new_failures < config.failure_threshold:
        return replace(state, consecutive_failures=new_failures)

    trip_count = state.trip_count + 1
    ceiling = min(
        config.base_cooldown_seconds * (config.backoff_multiplier ** (trip_count - 1)),
        config.max_cooldown_seconds,
    )
    cooldown = _equal_jitter_seconds(ceiling)
    return replace(
        state,
        consecutive_failures=new_failures,
        paused_until=now + timedelta(seconds=cooldown),
        trip_count=trip_count,
    )


def new_state(contract_name: str) -> CircuitGuardState:
    return CircuitGuardState(contract_name=contract_name)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
