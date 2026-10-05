"""
breaker/kafka_publisher.py

Publishes to remediation-requests (incidents allowed through) or
remediation-dlq (incidents blocked while the breaker is OPEN). Same
timeout settings discovered the hard way in Week 3's drift monitor:
kafka-python 3.0.11 defaults bootstrap_timeout_ms to 30000, separate from
request_timeout_ms/max_block_ms — all three need to be set explicitly or
an unreachable broker stalls the caller for 30+ seconds.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


def publish(bootstrap_servers: str, topic: str, event: Dict[str, Any]) -> None:
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
        future.get(timeout=10)
        logger.info("Published to %s: %s", topic, event)
    finally:
        producer.flush(timeout=5)
        producer.close(timeout=5)
