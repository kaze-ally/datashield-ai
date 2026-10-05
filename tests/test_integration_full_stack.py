"""
tests/test_integration_full_stack.py

A full-stack sanity check you can run BEFORE touching Docker: starts
circuit_breaker as a real subprocess HTTP server against your real
Postgres, drives it to a HALF_OPEN trial, then runs RCA Copilot's real
handle_message() against that trial with ONLY the Ollama call mocked (no
GPU/Ollama needed for this script) and verifies RCA's real HTTP call to
circuit_breaker's real /report-outcome actually advances the breaker's
real, persisted state correctly.

This validates the actual integration contract between the two services —
Kafka message shape, HTTP call shape, Postgres state transitions — without
needing a running Kafka broker or Ollama instance. It does NOT replace the
full Docker + real Kafka + real Ollama validation in TESTING_week4_part2.md;
think of this as the fast, iterate-in-seconds check, and the Docker guide
as the "does it work with all the real infrastructure" check.

Prerequisites:
  - A reachable Postgres with all three DDLs applied (drift_reports,
    circuit_breaker_state/events, rca_reports).
  - DATASHIELD_POSTGRES_DSN pointing at it.
  - Run from the repo root: `python tests/test_integration_full_stack.py`

Two scenarios: a confident low-severity diagnosis (should auto-reset the
breaker to CLOSED) and a high-severity diagnosis (should keep it OPEN,
escalated to a human).
"""
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
CB_DIR = REPO_ROOT / "services" / "circuit_breaker"
RCA_DIR = REPO_ROOT / "services" / "rca_copilot"
DSN = os.environ.get("DATASHIELD_POSTGRES_DSN")
CB_PORT = os.environ.get("CIRCUIT_BREAKER_TEST_PORT", "8021")  # a scratch port, separate from the real 8020
CB_URL = f"http://localhost:{CB_PORT}"

sys.path.insert(0, str(CB_DIR))
sys.path.insert(0, str(RCA_DIR))


def start_circuit_breaker() -> subprocess.Popen:
    env = os.environ.copy()
    env["DATASHIELD_POSTGRES_DSN"] = DSN
    env["BREAKER_CONFIG_PATH"] = str(CB_DIR / "config" / "circuit_breaker_config.yaml")
    env.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")  # unreachable is fine — dry_run bypasses publish anyway
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", CB_PORT],
        cwd=str(CB_DIR), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    for _ in range(30):
        try:
            r = requests.get(f"{CB_URL}/health", timeout=1)
            if r.status_code == 200:
                print(f"circuit_breaker up on port {CB_PORT}: {r.json()}")
                return proc
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(0.5)
    raise RuntimeError("circuit_breaker never came up — check its stdout for import/config errors")


def trip_and_age_to_half_open(monitor_name: str, report_id: str) -> None:
    from breaker.kafka_consumer import _handle_message
    from breaker.config import load_breaker_config
    from breaker.persistence import load_state, save_state

    config = load_breaker_config(str(CB_DIR / "config" / "circuit_breaker_config.yaml"))
    config.breaker.dry_run = True  # exercises the real state machine + Postgres, skips the real Kafka publish
    source = config.sources[0]

    for _ in range(config.breaker.failure_threshold):
        _handle_message(DSN, "localhost:9092", config, source, {"monitor_name": monitor_name, "report_id": report_id})

    state = load_state(DSN, config.outputs.postgres_state_table, monitor_name)
    assert state.state.value == "OPEN", f"expected OPEN after {config.breaker.failure_threshold} incidents, got {state.state.value}"

    aged = state.__class__(**{**state.__dict__, "opened_at": datetime.now(timezone.utc) - timedelta(seconds=state.cooldown_seconds + 5)})
    save_state(DSN, config.outputs.postgres_state_table, aged)

    _handle_message(DSN, "localhost:9092", config, source, {"monitor_name": monitor_name, "report_id": report_id})
    state = load_state(DSN, config.outputs.postgres_state_table, monitor_name)
    assert state.state.value == "HALF_OPEN", f"expected HALF_OPEN, got {state.state.value}"
    print(f"  breaker tripped and aged to HALF_OPEN for {monitor_name}")


def run_scenario(monitor_name: str, report_id: str, mock_rca_output, expected_final_state: str) -> None:
    print(f"\n=== Scenario: monitor={monitor_name}, expecting final state={expected_final_state} ===")
    trip_and_age_to_half_open(monitor_name, report_id)

    from rca.config import load_rca_config
    from rca.kafka_consumer import handle_message as rca_handle_message

    rca_config = load_rca_config(str(RCA_DIR / "config" / "rca_copilot_config.yaml"))
    rca_config.context.circuit_breaker_url = CB_URL  # point at this test's scratch port, not the real 8020

    payload = {"monitor_name": monitor_name, "report_id": report_id}

    with patch("rca.kafka_consumer.generate_rca", return_value=mock_rca_output) as mocked_llm:
        rca_handle_message(DSN, rca_config, payload)
        assert mocked_llm.called, "generate_rca was not called — RCA pipeline did not reach the LLM step"

    resp = requests.get(f"{CB_URL}/state/{monitor_name}", timeout=5)
    final_state = resp.json()
    print(f"  circuit_breaker state after RCA outcome report: {final_state}")
    assert final_state["state"] == expected_final_state, \
        f"expected breaker to end in {expected_final_state}, got {final_state['state']}"

    import psycopg2
    import psycopg2.extras
    with psycopg2.connect(DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM rca_reports WHERE monitor_name = %s ORDER BY created_at DESC LIMIT 1", (monitor_name,))
            row = cur.fetchone()
    assert row is not None, "no rca_reports row was persisted"
    print(f"  rca_reports persisted: severity={row['severity']}, confidence={row['confidence']}, outcome_success={row['outcome_success']}")
    print(f"  narrative: {row['narrative']}")
    print("  PASS")


def main() -> None:
    if not DSN:
        print("DATASHIELD_POSTGRES_DSN is not set.", file=sys.stderr)
        sys.exit(1)

    from rca.llm_client import RCAOutput

    cb_proc = start_circuit_breaker()
    try:
        run_scenario(
            monitor_name="orders_pipeline_drift",
            report_id=str(uuid.uuid4()),  # well-formed but unseeded — also exercises the "no matching
                                            # drift_reports row" graceful-degradation path in context_loader.py
            mock_rca_output=RCAOutput(
                root_cause_category="legitimate_business_shift",
                narrative="Order totals rose sharply alongside a new payment method, consistent with a promotional "
                          "campaign launch rather than a pipeline defect.",
                severity="low", confidence=0.87, model_used="mock-phi4-mini",
            ),
            expected_final_state="CLOSED",
        )

        run_scenario(
            monitor_name="checkout_pipeline_drift",
            report_id=str(uuid.uuid4()),
            mock_rca_output=RCAOutput(
                root_cause_category="upstream_data_bug",
                narrative="The new payment_method value appears malformed and correlates with a spike in null "
                          "shipping_country values, consistent with an upstream schema regression.",
                severity="high", confidence=0.91, model_used="mock-phi4-mini",
            ),
            expected_final_state="OPEN",
        )

        print("\n=== ALL SCENARIOS PASSED ===")
    finally:
        cb_proc.terminate()
        try:
            cb_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cb_proc.kill()
            cb_proc.wait(timeout=5)
        print("circuit_breaker subprocess stopped.")


if __name__ == "__main__":
    main()
