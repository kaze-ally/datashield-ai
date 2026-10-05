"""
breaker/kafka_consumer.py

Background consumer loop: subscribes to the configured source topics
(drift-events today; anomaly-events later), and for each message runs it
through the state machine, persists the result, and forwards it to either
remediation-requests or remediation-dlq.

Runs in a daemon thread started from main.py's FastAPI startup event, not
as the main process — the HTTP API (/health, /state, /report-outcome) needs
to stay responsive even if Kafka is momentarily unreachable, which is why
this loop retries its own connection independently rather than crashing
the whole service.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

from kafka import KafkaConsumer

from breaker.admission import BlockThrottle, EventAdmission
from breaker.config import CircuitBreakerConfig, SourceConfig
from breaker.kafka_publisher import publish
from breaker.persistence import load_state, log_event, save_state
from breaker.state_machine import Action, BreakerState, record_incident

logger = logging.getLogger(__name__)

RECONNECT_BACKOFF_SECONDS = 5

# Exposed via /health so a flood (or its suppression) is visible without log-diving.
STATS: Dict[str, int] = {"admitted": 0, "skipped_stale": 0, "skipped_duplicate": 0, "block_notifications_suppressed": 0}


@dataclass
class Guards:
    admission: EventAdmission
    throttle: BlockThrottle

    @classmethod
    def from_config(cls, config: CircuitBreakerConfig) -> "Guards":
        return cls(
            admission=EventAdmission(config.breaker.max_event_age_seconds),
            throttle=BlockThrottle(config.breaker.blocked_audit_interval_seconds),
        )
CONSUMER_POLL_TIMEOUT_MS = 3000  # lets the loop check stop_event periodically instead of blocking forever


def _handle_message(
    dsn: str,
    kafka_bootstrap: str,
    config: CircuitBreakerConfig,
    source: SourceConfig,
    payload: dict,
    guards: Optional[Guards] = None,
) -> None:
    monitor_name = payload.get(source.monitor_name_field)
    report_id = payload.get(source.report_id_field)
    if not monitor_name:
        logger.warning("Message on %s missing '%s' field, skipping: %s", source.topic, source.monitor_name_field, payload)
        return

    now = datetime.now(timezone.utc)
    if guards is not None:
        admit, why = guards.admission.check(report_id, payload.get("detected_at"), now)
        if not admit:
            STATS[f"skipped_{why}"] += 1
            logger.info("Skipping %s drift event for monitor=%s report_id=%s", why, monitor_name, report_id)
            return
        STATS["admitted"] += 1

    existing = load_state(dsn, config.outputs.postgres_state_table, monitor_name)
    current = existing or BreakerState(monitor_name=monitor_name)
    from_state = current.state.value

    decision = record_incident(current, config.breaker, now)
    is_transition = decision.new_state.state != current.state

    # "Still blocked" while OPEN changes nothing: skip the redundant write, and
    # notify (audit row + DLQ message) on the transition and then at most once
    # per blocked_audit_interval_seconds instead of once per replayed event.
    if decision.action == Action.BLOCK and guards is not None:
        if not guards.throttle.should_emit(monitor_name, time.monotonic(), is_transition):
            STATS["block_notifications_suppressed"] += 1
            return

    if decision.new_state != current:
        save_state(dsn, config.outputs.postgres_state_table, decision.new_state)
    log_event(
        dsn, config.outputs.postgres_events_table,
        monitor_name=monitor_name,
        event_type="incident_forwarded" if decision.action == Action.FORWARD else "incident_blocked",
        from_state=from_state, to_state=decision.new_state.state.value,
        reason=decision.reason, related_report_id=report_id,
    )

    if config.breaker.dry_run:
        logger.info(
            "[DRY RUN] Would %s for monitor=%s report_id=%s (%s)",
            decision.action.value, monitor_name, report_id, decision.reason,
        )
        return

    # State and audit event are already durably persisted above — a publish
    # failure here must not look like the breaker itself failed. Caught and
    # logged explicitly (not left to the consumer loop's generic per-message
    # handler) so this function is also safe to call from any future direct
    # entry point, not just the Kafka consumer loop.
    try:
        if decision.action == Action.FORWARD:
            publish(kafka_bootstrap, config.outputs.remediation_requests_topic, {
                "monitor_name": monitor_name,
                "report_id": report_id,
                "trigger_topic": source.topic,
                "breaker_state": decision.new_state.state.value,
                "forwarded_at": now.isoformat(),
                "incident": payload,
            })
        else:
            publish(kafka_bootstrap, config.outputs.remediation_dlq_topic, {
                "monitor_name": monitor_name,
                "report_id": report_id,
                "trigger_topic": source.topic,
                "reason": decision.reason,
                "blocked_at": now.isoformat(),
            })
            logger.warning(
                "ALERT: remediation blocked for monitor=%s (breaker %s) — report_id=%s: %s",
                monitor_name, decision.new_state.state.value, report_id, decision.reason,
            )
    except Exception:
        logger.exception(
            "Breaker decision for monitor=%s was persisted (state=%s) but the "
            "Kafka publish to notify downstream failed — check "
            "KAFKA_BOOTSTRAP_SERVERS. Not retrying inline to avoid blocking "
            "the consumer loop; the state/audit record is the source of truth.",
            monitor_name, decision.new_state.state.value,
        )


def run_consumer_loop(
    stop_event: threading.Event,
    dsn: str,
    kafka_bootstrap: str,
    config: CircuitBreakerConfig,
) -> None:
    topics = [s.topic for s in config.sources]
    if not topics:
        logger.warning("No source topics configured — consumer loop is a no-op.")
        return

    consumer: Optional[KafkaConsumer] = None
    guards = Guards.from_config(config)
    topic_by_name = {s.topic: s for s in config.sources}

    while not stop_event.is_set():
        try:
            if consumer is None:
                consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=kafka_bootstrap,
                    group_id="circuit-breaker",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    bootstrap_timeout_ms=10000,
                    request_timeout_ms=10000,
                    consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                )
                logger.info("Circuit breaker consumer connected, subscribed to %s", topics)

            for message in consumer:
                if stop_event.is_set():
                    break
                source = topic_by_name.get(message.topic)
                if source is None:
                    continue
                try:
                    _handle_message(dsn, kafka_bootstrap, config, source, message.value, guards)
                except Exception:
                    # One bad message must not kill the consumer loop for every
                    # other monitor — log loudly and keep consuming.
                    logger.exception("Failed to process message on %s: %s", message.topic, message.value)

        except Exception:
            logger.exception("Kafka consumer error — reconnecting in %ss", RECONNECT_BACKOFF_SECONDS)
            consumer = None
            time.sleep(RECONNECT_BACKOFF_SECONDS)

    if consumer is not None:
        consumer.close()
    logger.info("Circuit breaker consumer loop stopped.")
