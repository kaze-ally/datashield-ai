"""
scripts/publish_test_schema_mismatch.py

Same purpose as Week 4's `publish_test_remediation_request.py`: publish
directly to the source topic (schema-events) without needing the real
sidecar_validator to trip first — fastest way to exercise schema_healer's
Kafka -> Ollama -> Postgres path in isolation.

Usage (matches the docker compose run pattern in TESTING_week4_part2.md):

    docker compose run --rm \
      -e KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
      -v ${PWD}/scripts:/scripts \
      schema_healer python /scripts/publish_test_schema_mismatch.py --contract-name orders_v1
"""
from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-name", default="orders_v1")
    parser.add_argument("--topic", default="schema-events")
    args = parser.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Simulates an upstream rename (order_amt instead of order_total) plus a
    # new, previously-unseen field — the two most common real-world schema
    # drift patterns per the contract-mapping literature cited in
    # README_week5_schema_healer.md.
    event = {
        "event_type": "schema_mismatch",
        "report_id": str(uuid.uuid4()),
        "contract_name": args.contract_name,
        "record": {
            "order_id": "test-order-001",
            "order_amt": "89.99",
            "payment_method": "credit_card",
            "loyalty_tier": "gold",  # not in the contract
        },
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }

    producer.send(args.topic, value=event)
    producer.flush(timeout=5)
    print(f"published schema_mismatch event report_id={event['report_id']} to {args.topic}")


if __name__ == "__main__":
    main()
