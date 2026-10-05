"""tests/test_circuit_guard.py — no infra needed."""
from datetime import datetime, timedelta, timezone

from services.schema_healer.healer.circuit_guard import (
    CircuitGuardConfig,
    check,
    new_state,
    record_failure,
    record_success,
)

NOW = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)


def _config(**overrides):
    base = dict(
        failure_threshold=3,
        base_cooldown_seconds=100.0,
        backoff_multiplier=2.0,
        max_cooldown_seconds=3600.0,
    )
    base.update(overrides)
    return CircuitGuardConfig(**base)


def test_allows_by_default():
    state = new_state("orders_v1")
    assert check(state, NOW).allowed is True


def test_failures_below_threshold_do_not_pause():
    config = _config()
    state = new_state("orders_v1")
    state = record_failure(state, NOW, config)
    state = record_failure(state, NOW, config)
    assert state.consecutive_failures == 2
    assert state.paused_until is None
    assert check(state, NOW).allowed is True


def test_threshold_crossed_trips_the_guard():
    config = _config(failure_threshold=3)
    state = new_state("orders_v1")
    for _ in range(3):
        state = record_failure(state, NOW, config)
    assert state.consecutive_failures == 3
    assert state.paused_until is not None
    decision = check(state, NOW)
    assert decision.allowed is False
    assert "healer_paused" in decision.reason


def test_cooldown_never_returns_near_zero_right_after_tripping():
    # Same regression concern as circuit_breaker's Week 4 fix: full jitter
    # (random(0, ceiling)) can draw near zero. Equal jitter guarantees at
    # least half the computed ceiling. Sample many draws to guard against
    # this silently regressing back to full jitter.
    config = _config(failure_threshold=1, base_cooldown_seconds=200.0)
    floor = config.base_cooldown_seconds / 2
    for _ in range(200):
        state = new_state("orders_v1")
        state = record_failure(state, NOW, config)
        cooldown = (state.paused_until - NOW).total_seconds()
        assert cooldown >= floor, f"cooldown {cooldown} fell below equal-jitter floor {floor}"
        assert cooldown <= config.base_cooldown_seconds


def test_backoff_grows_on_repeated_trips():
    config = _config(failure_threshold=1, base_cooldown_seconds=100.0, backoff_multiplier=2.0)
    state = new_state("orders_v1")
    state = record_failure(state, NOW, config)  # trip #1, ceiling ~100
    first_cooldown = (state.paused_until - NOW).total_seconds()

    later = state.paused_until + timedelta(seconds=1)
    state = record_failure(state, later, config)  # trip #2, ceiling ~200
    second_cooldown = (state.paused_until - later).total_seconds()

    assert second_cooldown > first_cooldown


def test_backoff_capped_at_max_cooldown():
    # backoff_multiplier=10 would blow past max_cooldown_seconds within a
    # couple of trips if uncapped; assert each individual trip's cooldown
    # (not the cumulative elapsed time across trips) stays under the cap.
    config = _config(
        failure_threshold=1, base_cooldown_seconds=1000.0, backoff_multiplier=10.0, max_cooldown_seconds=1500.0
    )
    state = new_state("orders_v1")
    clock = NOW
    for _ in range(4):
        clock = state.paused_until + timedelta(seconds=1) if state.paused_until else clock
        state = record_failure(state, clock, config)
        this_trip_cooldown = (state.paused_until - clock).total_seconds()
        assert this_trip_cooldown <= config.max_cooldown_seconds


def test_success_resets_guard_fully():
    config = _config(failure_threshold=2)
    state = new_state("orders_v1")
    state = record_failure(state, NOW, config)
    state = record_failure(state, NOW, config)
    assert state.paused_until is not None

    state = record_success(state)
    assert state.consecutive_failures == 0
    assert state.paused_until is None
    assert check(state, NOW).allowed is True


def test_guard_allows_again_once_cooldown_elapses():
    config = _config(failure_threshold=1, base_cooldown_seconds=60.0)
    state = new_state("orders_v1")
    state = record_failure(state, NOW, config)
    assert check(state, NOW).allowed is False

    much_later = state.paused_until + timedelta(seconds=1)
    assert check(state, much_later).allowed is True
