"""
services/schema_healer/healer/kafka_io.py

Consumer + publisher, following the same "background thread inside a
FastAPI process" shape as circuit_breaker/breaker/kafka_consumer.py and
rca_copilot/rca/kafka_consumer.py — /health can report `consumer_alive`
the same way those two already do.

The sidecar publishes contract violations to the dedicated
`schema-evolution-requests` topic and this consumer expects messages shaped
like:

    {
      "event_type": "schema_mismatch",
      "report_id": "<uuid, optional — generated if absent>",
      "contract_name": "orders_v1",
      "record": { ...the raw offending JSON payload... },
      "detected_at": "2026-09-22T12:00:00Z"
    }

Since ai_catalog also reads this topic (for lineage/schema cataloging, a
different concern), this consumer filters on `event_type == "schema_mismatch"`
and ignores everything else on the topic rather than assuming it's the only
consumer or the only message shape present. If your actual
sidecar_validator emits a different shape or a dedicated topic instead,
update `PARSE` below and the `SCHEMA_EVENTS_TOPIC` env var — the rest of
the pipeline (pipeline.py, mapping_ops.py) doesn't care where the record
came from.
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("schema_healer.kafka_io")


def parse_schema_mismatch_message(raw: dict) -> dict | None:
    """Returns a normalized {report_id, contract_name, record} dict, or
    None if this message isn't a schema-mismatch event this service should
    act on."""
    if raw.get("event_type") != "schema_mismatch":
        return None
    contract_name = raw.get("contract_name")
    record = raw.get("record")
    if not contract_name or not isinstance(record, dict):
        logger.warning("schema_mismatch event missing contract_name/record: %s", raw)
        return None
    return {
        "report_id": raw.get("report_id") or str(uuid.uuid4()),
        "contract_name": contract_name,
        "record": record,
    }


class SchemaHealerPublisher:
    def __init__(self, bootstrap_servers: str, validated_topic: str, dlq_topic: str):
        self.validated_topic = validated_topic
        self.dlq_topic = dlq_topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )

    def publish_healed(self, record: dict, lineage: dict) -> None:
        envelope = {**record, "_schema_healer": lineage}
        try:
            self._producer.send(self.validated_topic, value=envelope)
            self._producer.flush(timeout=5)
        except KafkaError as e:
            logger.error("failed to publish healed record to %s: %s", self.validated_topic, e)
            raise

    def publish_dlq(self, original_record: dict, lineage: dict) -> None:
        envelope = {"record": original_record, "_schema_healer": lineage}
        try:
            self._producer.send(self.dlq_topic, value=envelope)
            self._producer.flush(timeout=5)
        except KafkaError as e:
            # Matches the fix documented in README_week4.md: don't let a publish
            # failure escape uncaught just because this isn't inside the
            # consumer loop's own generic try/except.
            logger.error("failed to publish to DLQ topic %s: %s", self.dlq_topic, e)


class SchemaHealerConsumer:
    """Background thread: consume schema-evolution-requests and call `handle_message` for
    every normalized schema_mismatch event. `handle_message` is injected so
    the FastAPI app can wire in the real pipeline while tests can inject a
    stub."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        handle_message,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.handle_message = handle_message
        self._alive = False
        self._thread: threading.Thread | None = None

    @property
    def consumer_alive(self) -> bool:
        return self._alive

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self._alive = True
        logger.info("schema_healer consumer started on topic=%s group=%s", self.topic, self.group_id)
        try:
            for message in consumer:
                try:
                    normalized = parse_schema_mismatch_message(message.value)
                    if normalized is not None:
                        self.handle_message(normalized)
                except Exception:  # noqa: BLE001 — one bad message must not kill the loop
                    logger.exception("error handling schema-evolution-requests message, skipping")
        finally:
            self._alive = False


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
