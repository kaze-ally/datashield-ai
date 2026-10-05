"""
drift_detector/kafka_publisher.py

Publishes drift events to a dedicated, low-volume `drift-events` Kafka topic
— the same topic-separation principle used for the schema-events/catalog
topic feeding ai_catalog, so drift-check traffic never competes with or
backs up behind the high-volume validated-orders stream.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


def publish_drift_event(bootstrap_servers: str, topic: str, event: Dict[str, Any]) -> None:
    """
    Note from live testing against kafka-python 3.0.11 specifically: this
    version defaults bootstrap_timeout_ms to 30000 — a separate setting
    from request_timeout_ms/max_block_ms, and the one that actually governs
    how long __init__ blocks against an unreachable broker. Missing it left
    a 30+ second stall in place even with the other two timeouts lowered.
    All three are set here so a Kafka outage fails in ~5s, not 30+.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
        bootstrap_timeout_ms=5000,
        request_timeout_ms=5000,
        max_block_ms=5000,
    )
    try:
        future = producer.send(topic, value=event)
        future.get(timeout=10)  # block for delivery confirmation — this is a safety signal, not telemetry
        logger.info("Published drift event to %s: %s", topic, event.get("report_id"))
    finally:
        producer.flush(timeout=5)
        producer.close(timeout=5)
