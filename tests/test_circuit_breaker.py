"""
tests/test_circuit_breaker.py

Unit tests for the pure state machine (breaker/state_machine.py) — no
Postgres, no Kafka, no Docker. This is the safety-critical logic, so it's
tested exhaustively: every state transition in both directions, plus the
two edge cases that actually matter in production (repeated trips growing
the backoff, and a stale HALF_OPEN with no trial recorded after a restart).
"""
from datetime import datetime, timedelta, timezone

import pytest

from breaker.config import BreakerTuning
from breaker.state_machine import (
    Action,
    BreakerState,
    State,
    record_incident,
    record_outcome,
)


@pytest.fixture
def tuning() -> BreakerTuning:
    return BreakerTuning(
        failure_threshold=3,
        cooldown_seconds=100.0,
        cooldown_backoff_multiplier=2.0,
        cooldown_max_seconds=1000.0,
        cooldown_jitter_seconds=0.0,  # deterministic for tests
    )


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_closed_forwards_below_threshold(tuning):
    state = BreakerState(monitor_name="m1")
    decision = record_incident(state, tuning, NOW)
    assert decision.action == Action.FORWARD
    assert decision.new_state.state == State.CLOSED
    assert decision.new_state.consecutive_failures == 1


def test_closed_trips_open_at_threshold(tuning):
    state = BreakerState(monitor_name="m1", consecutive_failures=2)  # one more hits threshold=3
    decision = record_incident(state, tuning, NOW)
    assert decision.action == Action.BLOCK
    assert decision.new_state.state == State.OPEN
    assert decision.new_state.trip_count == 1
    assert decision.new_state.opened_at == NOW
    assert 0 <= decision.new_state.cooldown_seconds <= tuning.cooldown_seconds  # first trip, no backoff growth yet


def test_open_blocks_before_cooldown_elapses(tuning):
    state = BreakerState(
        monitor_name="m1", state=State.OPEN, consecutive_failures=3,
        trip_count=1, opened_at=NOW, cooldown_seconds=100.0,
    )
    decision = record_incident(state, tuning, NOW + timedelta(seconds=50))
    assert decision.action == Action.BLOCK
    assert decision.new_state.state == State.OPEN


def test_open_transitions_to_half_open_after_cooldown(tuning):
    state = BreakerState(
        monitor_name="m1", state=State.OPEN, consecutive_failures=3,
        trip_count=1, opened_at=NOW, cooldown_seconds=100.0,
    )
    decision = record_incident(state, tuning, NOW + timedelta(seconds=101))
    assert decision.action == Action.FORWARD
    assert decision.new_state.state == State.HALF_OPEN
    assert decision.new_state.half_open_trial_in_flight is True


def test_half_open_blocks_second_incident_while_trial_in_flight(tuning):
    state = BreakerState(
        monitor_name="m1", state=State.HALF_OPEN, trip_count=1,
        half_open_trial_in_flight=True,
    )
    decision = record_incident(state, tuning, NOW)
    assert decision.action == Action.BLOCK
    assert decision.new_state.state == State.HALF_OPEN


def test_half_open_success_resets_to_closed(tuning):
    state = BreakerState(
        monitor_name="m1", state=State.HALF_OPEN, trip_count=1,
        half_open_trial_in_flight=True, consecutive_failures=5,
    )
    new_state = record_outcome(state, tuning, success=True, now=NOW)
    assert new_state.state == State.CLOSED
    assert new_state.consecutive_failures == 0
    assert new_state.trip_count == 0
    assert new_state.opened_at is None


def test_half_open_failure_reopens_with_grown_backoff(tuning):
    state = BreakerState(
        monitor_name="m1", state=State.HALF_OPEN, trip_count=1,
        half_open_trial_in_flight=True,
    )
    new_state = record_outcome(state, tuning, success=False, now=NOW)
    assert new_state.state == State.OPEN
    assert new_state.trip_count == 2
    # Equal jitter guarantees at least half the computed backoff, unlike
    # full jitter which could draw near zero — so this asserts the floor,
    # not an exact value.
    # second trip ceiling: base * multiplier^(2-1) = 100 * 2 = 200s (jitter=0 in this fixture)
    assert 100.0 <= new_state.cooldown_seconds <= 200.0


def test_cooldown_never_returns_near_zero_right_after_tripping(tuning):
    """
    The specific bug equal-jitter fixes: a circuit breaker cooldown must
    never draw a value close to zero, or a trial gets let straight back
    through right after tripping and the cooldown is pointless. Sampled
    repeatedly since this is randomized — a single lucky draw wouldn't
    catch a regression back to full jitter.
    """
    state = BreakerState(monitor_name="m1", consecutive_failures=2)
    for _ in range(200):
        decision = record_incident(state, tuning, NOW)
        assert decision.new_state.cooldown_seconds >= tuning.cooldown_seconds / 2


def test_cooldown_ceiling_grows_monotonically_with_trip_count(tuning):
    """
    Tests the growth invariant directly against _compute_cooldown's ceiling
    rather than trusting individual random draws to be monotonic — two
    independent jittered samples aren't guaranteed ordered even when the
    underlying ceiling they're drawn from is strictly increasing.
    """
    from breaker.state_machine import _compute_cooldown

    ceilings = []
    for trip_count in range(1, 6):
        samples = [_compute_cooldown(tuning, trip_count) for _ in range(500)]
        ceilings.append(max(samples))  # empirical ceiling across many draws

    assert ceilings == sorted(ceilings), f"Cooldown ceiling must grow with trip_count, got {ceilings}"
    assert all(c <= tuning.cooldown_max_seconds + tuning.cooldown_jitter_seconds for c in ceilings)


def test_outcome_reported_while_closed_is_a_noop(tuning):
    state = BreakerState(monitor_name="m1", state=State.CLOSED, consecutive_failures=1)
    new_state = record_outcome(state, tuning, success=False, now=NOW)
    assert new_state == state  # stale/duplicate callback shouldn't perturb a healthy breaker


def test_half_open_with_no_trial_recorded_allows_one_trial(tuning):
    """Defensive path: a restart could load HALF_OPEN state with the in-flight flag lost."""
    state = BreakerState(monitor_name="m1", state=State.HALF_OPEN, trip_count=1, half_open_trial_in_flight=False)
    decision = record_incident(state, tuning, NOW)
    assert decision.action == Action.FORWARD
    assert decision.new_state.half_open_trial_in_flight is True


def test_cooldown_never_exceeds_configured_cap_even_after_many_trips(tuning):
    state = BreakerState(monitor_name="m1", state=State.HALF_OPEN, trip_count=1, half_open_trial_in_flight=True)
    for _ in range(20):  # far more trips than needed to blow past the cap without one
        state = record_outcome(state, tuning, success=False, now=NOW)
        assert state.cooldown_seconds <= tuning.cooldown_max_seconds
        decision = record_incident(state, tuning, NOW + timedelta(seconds=state.cooldown_seconds + 1))
        state = decision.new_state
