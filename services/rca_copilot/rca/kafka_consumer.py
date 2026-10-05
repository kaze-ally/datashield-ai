"""
rca/kafka_consumer.py

Consumes remediation-requests, runs the full RCA pipeline per message:
load context -> call LLM -> decide outcome -> persist -> report back to
the circuit breaker. Same reconnect-loop shape as circuit_breaker's
consumer (Week 4 part 1) — retries its own Kafka connection independently
so the HTTP API stays responsive if Kafka is briefly down.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaConsumer

from rca.circuit_breaker_client import report_outcome
from rca.config import RCAConfig
from rca.context_loader import Context, load_context
from rca.llm_client import generate_rca
from rca.outcome_policy import decide_outcome
from rca.persistence import persist_rca_report

logger = logging.getLogger(__name__)

RECONNECT_BACKOFF_SECONDS = 5
CONSUMER_POLL_TIMEOUT_MS = 3000


def _summarize_context(context: Context) -> str:
    lines = [f"Monitor: {context.monitor_name}", f"Report ID: {context.report_id}"]
    if context.drift_report:
        dr = context.drift_report
        lines.append(f"Drift detected: {dr.get('dataset_drift_detected')}, drift_share: {dr.get('drift_share')}")
        lines.append(f"Window: reference [{dr.get('reference_start')} .. {dr.get('reference_end')}], "
                      f"current [{dr.get('current_start')} .. {dr.get('current_end')}]")
        col_results = dr.get("column_results")
        if col_results:
            lines.append(f"Per-column results: {json.dumps(col_results)[:2000]}")  # bounded, avoid unbounded prompt growth
    else:
        lines.append("No matching drift_reports row found — proceeding with limited context.")

    if context.incident:
        incident = context.incident
        lines.append(f"Incident source topic: {incident.get('source_topic', 'unknown')}")
        if incident.get("event_type") == "anomaly_detected":
            lines.append(
                "Isolation Forest anomaly: "
                f"order_id={incident.get('order_id')}, "
                f"anomaly_score={incident.get('anomaly_score')}, "
                f"features={json.dumps(incident.get('features', {}), sort_keys=True)[:1000]}"
            )
        else:
            lines.append(f"Incident payload: {json.dumps(incident, sort_keys=True)[:1200]}")

    if context.breaker_history:
        recent = [f"{e['event_type']} ({e['from_state']}->{e['to_state']})" for e in context.breaker_history[:5]]
        lines.append(f"Recent breaker history for this monitor: {'; '.join(recent)}")

    return "\n".join(lines)


def handle_message(dsn: str, config: RCAConfig, payload: dict) -> None:
    monitor_name = payload.get(config.source.monitor_name_field)
    report_id = payload.get(config.source.report_id_field)
    if not monitor_name:
        logger.warning("remediation-requests message missing '%s', skipping: %s", config.source.monitor_name_field, payload)
        return

    context = load_context(
        dsn, config.context.drift_reports_table, config.context.circuit_breaker_url,
        monitor_name, report_id, payload.get("incident"),
    )
    context_summary = _summarize_context(context)

    rca = generate_rca(
        base_url=config.llm.base_url, model=config.llm.model,
        fallback_model=config.llm.fallback_model, timeout_seconds=config.llm.timeout_seconds,
        context_summary=context_summary,
    )

    decision = decide_outcome(rca, config.outcome_policy)

    now = datetime.now(timezone.utc)
    try:
        persist_rca_report(
            dsn, config.outputs.postgres_table,
            monitor_name=monitor_name, report_id=report_id,
            root_cause_category=rca.root_cause_category, narrative=rca.narrative,
            severity=rca.severity, confidence=rca.confidence, model_used=rca.model_used,
            outcome_success=decision.success, outcome_reason=decision.reason,
            context_snapshot={
                "drift_report": context.drift_report,
                "breaker_history": context.breaker_history,
                "incident": context.incident,
            },
            created_at=now,
        )
    except Exception:
        # Persistence failing must not stop the outcome report — the breaker
        # needs to hear back regardless, or a HALF_OPEN trial gets stuck.
        logger.exception("Failed to persist RCA report for monitor=%s — reporting outcome to breaker anyway", monitor_name)

    logger.info(
        "RCA for monitor=%s: category=%s severity=%s confidence=%.2f -> outcome=%s (%s)",
        monitor_name, rca.root_cause_category, rca.severity, rca.confidence, decision.success, decision.reason,
    )

    report_outcome(config.context.circuit_breaker_url, monitor_name, report_id, decision.success)


def run_consumer_loop(stop_event: threading.Event, dsn: str, kafka_bootstrap: str, config: RCAConfig) -> None:
    consumer: Optional[KafkaConsumer] = None

    while not stop_event.is_set():
        try:
            if consumer is None:
                consumer = KafkaConsumer(
                    config.source.topic,
                    bootstrap_servers=kafka_bootstrap,
                    group_id="rca-copilot",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    bootstrap_timeout_ms=10000,
                    request_timeout_ms=10000,
                    consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                )
                logger.info("RCA Copilot consumer connected, subscribed to %s", config.source.topic)

            for message in consumer:
                if stop_event.is_set():
                    break
                try:
                    handle_message(dsn, config, message.value)
                except Exception:
                    logger.exception("Failed to process remediation-request: %s", message.value)

        except Exception:
            logger.exception("Kafka consumer error — reconnecting in %ss", RECONNECT_BACKOFF_SECONDS)
            consumer = None
            time.sleep(RECONNECT_BACKOFF_SECONDS)

    if consumer is not None:
        consumer.close()
    logger.info("RCA Copilot consumer loop stopped.")
