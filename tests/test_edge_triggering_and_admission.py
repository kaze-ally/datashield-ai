"""
Regression tests for the 2026-10-05 drift-event flood:
  * breaker/state_machine.py  - blocked incidents are not failures
  * breaker/admission.py      - stale / duplicate rejection, block throttling
  * drift_detector/publish_gate.py - edge-triggered publishing with heartbeat
"""
import datetime as dt
from datetime import datetime, timedelta, timezone

from breaker.admission import BlockThrottle, EventAdmission, parse_event_time
from breaker.config import BreakerTuning
from breaker.state_machine import Action, BreakerState, State, record_incident
from drift_detector.publish_gate import DriftPublishGate

NOW = datetime(2026, 10, 5, 4, 0, tzinfo=timezone.utc)
TUNING = BreakerTuning(failure_threshold=3, cooldown_seconds=300, cooldown_jitter_seconds=0)


# ---------------- state machine ----------------
def test_blocked_incidents_while_open_do_not_inflate_failure_count():
    s = BreakerState(monitor_name="m")
    for _ in range(3):
        s = record_incident(s, TUNING, NOW).new_state
    assert s.state == State.OPEN and s.consecutive_failures == 3 and s.trip_count == 1
    for i in range(13):  # the 13 extra events seen in the live flood
        d = record_incident(s, TUNING, NOW + timedelta(seconds=i + 1))
        assert d.action == Action.BLOCK
        s = d.new_state
    assert s.consecutive_failures == 3          # was 16 before the fix
    assert s.trip_count == 1


def test_half_open_in_flight_blocks_without_changing_state():
    s = BreakerState(monitor_name="m", state=State.HALF_OPEN, half_open_trial_in_flight=True,
                     consecutive_failures=3, trip_count=1)
    d = record_incident(s, TUNING, NOW)
    assert d.action == Action.BLOCK and d.new_state == s


# ---------------- admission ----------------
def test_stale_events_are_rejected_fresh_admitted():
    adm = EventAdmission(max_age_seconds=900)
    old = (NOW - timedelta(hours=5)).replace(tzinfo=None).isoformat()   # drift monitor emits naive UTC
    fresh = (NOW - timedelta(seconds=30)).replace(tzinfo=None).isoformat()
    assert adm.check("r1", old, NOW) == (False, "stale")
    assert adm.check("r2", fresh, NOW) == (True, "ok")


def test_duplicate_report_id_counted_once():
    adm = EventAdmission(max_age_seconds=900)
    assert adm.check("r1", None, NOW)[0] is True
    assert adm.check("r1", None, NOW) == (False, "duplicate")


def test_missing_or_garbage_timestamp_fails_open():
    adm = EventAdmission(max_age_seconds=900)
    assert adm.check("a", None, NOW)[0] is True
    assert adm.check("b", "not-a-date", NOW)[0] is True
    assert parse_event_time("2026-10-05T04:00:00Z") == NOW


def test_age_check_can_be_disabled():
    adm = EventAdmission(max_age_seconds=0)
    assert adm.check("r", "2020-01-01T00:00:00", NOW)[0] is True


def test_block_throttle_emits_on_transition_then_once_per_interval():
    t = BlockThrottle(interval_seconds=60)
    assert t.should_emit("m", 0.0, is_transition=True) is True     # the trip itself
    assert t.should_emit("m", 1.0, is_transition=False) is False   # replayed duplicates
    assert t.should_emit("m", 59.0, is_transition=False) is False
    assert t.should_emit("m", 61.0, is_transition=False) is True   # periodic reminder
    assert t.should_emit("other", 62.0, is_transition=False) is True  # per-monitor


# ---------------- publish gate ----------------
def _t(minutes):  # naive UTC like the service
    return dt.datetime(2026, 10, 5, 4, 0) + dt.timedelta(minutes=minutes)


def test_gate_publishes_edge_then_suppresses_repeats_then_heartbeats():
    g = DriftPublishGate(renotify_seconds=1800, min_publish_interval_seconds=120)
    assert g.decide("m", True, _t(0)) == (True, "new drift episode")
    assert g.decide("m", True, _t(5))[0] is False      # second DAG, same hour
    assert g.decide("m", True, _t(60 - 31))[0] is False
    ok, why = g.decide("m", True, _t(31))
    assert ok and "heartbeat" in why


def test_gate_clean_check_rearms_the_edge_but_flaps_are_damped():
    g = DriftPublishGate(renotify_seconds=1800, min_publish_interval_seconds=120)
    assert g.decide("m", True, _t(0))[0] is True
    assert g.decide("m", False, _t(1)) == (False, "no drift")
    assert g.decide("m", True, _t(1.5))[0] is False     # < 120s since last publish -> flap damped
    assert g.decide("m", False, _t(10))[0] is False
    assert g.decide("m", True, _t(11))[0] is True       # genuinely new episode


def test_gate_force_bypasses_and_failed_publish_can_retry():
    g = DriftPublishGate()
    assert g.decide("m", True, _t(0))[0] is True
    assert g.decide("m", True, _t(1), force=True) == (True, "forced")
    g.forget_publish("m")
    assert g.decide("m", True, _t(2))[0] is True        # publish had failed -> not gated out


def test_gate_never_publishes_without_drift():
    g = DriftPublishGate()
    assert g.decide("m", False, _t(0), force=True)[0] is False
