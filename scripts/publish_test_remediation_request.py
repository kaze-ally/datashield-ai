"""
scripts/publish_test_remediation_request.py

Publishes a synthetic remediation-requests message so you can test RCA
Copilot's full pipeline (context load -> real Ollama call -> outcome
policy -> persist -> report to circuit_breaker) WITHOUT first tripping
the circuit breaker through 3 real drift incidents each time. Reuses
whatever's already installed in the rca_copilot image (kafka-python).

Usage:
    python scripts/publish_test_remediation_request.py --monitor-name orders_pipeline_drift

Reads KAFKA_BOOTSTRAP_SERVERS from the environment.

Note: circuit_breaker will only have a HALF_OPEN trial waiting on an
outcome if you've actually tripped it first (see TESTING_week4_part2.md
Level 6). Publishing here without that just exercises RCA Copilot's own
pipeline in isolation — its /report-outcome call will get a 404 from
circuit_breaker if that monitor has no state yet, which is expected and
fine for testing RCA Copilot alone.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid

from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--monitor-name", default="orders_pipeline_drift")
    parser.add_argument("--topic", default="remediation-requests")
    parser.add_argument("--report-id", default=None, help="Defaults to a fresh random UUID.")
    args = parser.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        print("KAFKA_BOOTSTRAP_SERVERS is not set.", file=sys.stderr)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        bootstrap_timeout_ms=5000,
        request_timeout_ms=5000,
        max_block_ms=5000,
    )

    event = {
        "monitor_name": args.monitor_name,
        "report_id": args.report_id or str(uuid.uuid4()),
        "trigger_topic": "drift-events",
        "breaker_state": "HALF_OPEN",
        "forwarded_at": None,
    }

    try:
        future = producer.send(args.topic, value=event)
        future.get(timeout=10)
        print(f"Published remediation-request: {event}")
    finally:
        producer.flush(timeout=5)
        producer.close(timeout=5)


if __name__ == "__main__":
    main()
