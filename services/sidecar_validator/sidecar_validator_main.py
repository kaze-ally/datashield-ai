"""
DataShield AI — Sidecar Data Contract Validator (Week 2, Paper 9)
===================================================================
Implements the audit stage of a Write-Audit-Publish pipeline: `raw-data`
is the fronting/staging topic, this sidecar is the audit, and
`validated-data` is the publish topic. Netflix's Data Mesh uses the same
two-phase fronting-Kafka shape; the difference here is that the audit logic
lives in a standalone sidecar driven entirely by YAML (Paper 9's declarative
contract model), not hand-written inside a Flink/Spark job per topic.

Why a sidecar instead of Confluent Schema Registry / broker-side rules:
  - Community-tier schema registries only validate client-side, inside the
    producer SDK — anything that speaks the Kafka wire protocol directly, or
    ships a stale client, bypasses enforcement entirely. Broker-side
    enforcement is a paid tier on Confluent.
  - Registries also only cover *structural* schema (types, required fields),
    not the *semantic* rules DataShield actually needs: range checks,
    freshness SLAs, allowed-value sets, and per-field PII handling. Grab's
    Coban platform draws this exact syntactic-vs-semantic line and solves it
    with a standalone contract-testing layer — this module is that layer for
    DataShield.
  - Consumer-side, topic-level enforcement also means every record is
    audited regardless of which producer (or bypass) wrote it, closing the
    gap the registry-only approach leaves open.

Routing on a violation follows the contract's own `on_violation.action`
(see /contracts/examples/orders_v1.yaml) — usually `route_to_healer`, which
lands the record on `schema-evolution-requests` for Week 3's LLM Schema
Healer. Only contracts with no configured healing path go straight to the
dead-letter queue. Every decision — pass, healed-route, or DLQ — is written
to Postgres `pipeline_events` so nothing disappears silently (an
unobserved DLQ is one of the most common "silent failure" complaints in
production Kafka data-quality writeups).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import yaml
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sidecar_validator")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_APP_DSN = os.environ.get("POSTGRES_APP_DSN", "")
CONTRACTS_DIR = Path(os.environ.get("CONTRACTS_DIR", "/app/contracts"))
SCHEMA_EVENTS_TOPIC = os.environ.get("SCHEMA_EVENTS_TOPIC", "schema-events")
DLQ_TOPIC_DEFAULT = os.environ.get("DLQ_TOPIC_DEFAULT", "dead-letter-queue")

app = FastAPI(title="DataShield Sidecar Validator")
_contracts: dict[str, dict[str, Any]] = {}          # keyed by source_topic
_last_fingerprint: dict[str, str] = {}              # keyed by contract name
_producer: KafkaProducer | None = None
_producer_lock = threading.Lock()


# --------------------------------------------------------------------------
# Contract loading
# --------------------------------------------------------------------------
def load_contracts(directory: Path) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    for path in directory.rglob("*.yaml"):
        with open(path) as f:
            doc = yaml.safe_load(f)
        contract = doc.get("contract", {})
        source_topic = contract.get("kafka", {}).get("source_topic")
        if not source_topic:
            logger.warning("Skipping %s: no kafka.source_topic defined", path)
            continue
        registry[source_topic] = doc
        logger.info("Loaded contract '%s' v%s -> topic '%s'", contract.get("name"), contract.get("version"), source_topic)
    return registry


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------
_TYPE_CHECKERS = {
    "string": lambda v: isinstance(v, str),
    "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "timestamp": lambda v: isinstance(v, str),  # parsed separately for freshness
}


def validate_syntax(record: dict[str, Any], fields: list[dict[str, Any]]) -> list[str]:
    """Structural checks: required fields present, correct type, allowed values."""
    violations: list[str] = []
    for field in fields:
        name = field["name"]
        value = record.get(name)
        if field.get("required") and value is None:
            violations.append(f"missing_required_field:{name}")
            continue
        if value is None:
            continue
        checker = _TYPE_CHECKERS.get(field.get("type", "string"))
        if checker and not checker(value):
            violations.append(f"type_mismatch:{name}:expected_{field.get('type')}")
        allowed = field.get("allowed_values")
        if allowed and value not in allowed:
            violations.append(f"invalid_value:{name}:{value}")
    return violations


def validate_quality(record: dict[str, Any], rules: list[dict[str, Any]]) -> list[str]:
    """Semantic checks: not-null, numeric ranges, freshness SLAs."""
    violations: list[str] = []
    for rule in rules:
        kind = rule["rule"]
        if kind == "not_null":
            for name in rule.get("fields", []):
                if record.get(name) is None:
                    violations.append(f"not_null:{name}")
        elif kind == "range_check":
            name = rule["field"]
            value = record.get(name)
            if isinstance(value, (int, float)):
                if "min" in rule and value < rule["min"]:
                    violations.append(f"range_check:{name}:below_min")
                if "max" in rule and value > rule["max"]:
                    violations.append(f"range_check:{name}:above_max")
        elif kind == "freshness":
            name = rule["field"]
            raw = record.get(name)
            if raw:
                try:
                    ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                    delay_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60
                    if delay_min > rule.get("max_delay_minutes", 15):
                        violations.append(f"freshness:{name}:delay_{int(delay_min)}m")
                except ValueError:
                    violations.append(f"freshness:{name}:unparseable_timestamp")
    return violations


def mask_pii(record: dict[str, Any], fields: list[dict[str, Any]]) -> dict[str, Any]:
    """Deterministic structured-field masking, per the contract's declared strategy.

    Complements — doesn't replace — the NLP-based masking planned for
    free-text fields; this handles the fields the contract already knows
    are PII, before the record ever reaches an unmasked downstream topic.
    """
    masked = dict(record)
    for field in fields:
        if not field.get("pii"):
            continue
        name = field["name"]
        value = masked.get(name)
        if value is None:
            continue
        strategy = field.get("masking_strategy", "hash")
        if strategy == "hash":
            masked[name] = hashlib.sha256(str(value).encode()).hexdigest()[:16]
        elif strategy == "redact":
            masked[name] = "***"
        else:
            logger.warning("Unknown masking_strategy '%s' for field '%s' — leaving unmasked", strategy, name)
    return masked


def schema_fingerprint(fields: list[dict[str, Any]]) -> tuple[str, dict[str, str]]:
    shape = {f["name"]: f.get("type", "string") for f in fields}
    fp = hashlib.sha256(json.dumps(shape, sort_keys=True).encode()).hexdigest()[:16]
    return fp, shape


# --------------------------------------------------------------------------
# Kafka + Postgres plumbing
# --------------------------------------------------------------------------
def get_producer() -> KafkaProducer:
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
        return _producer


def log_pipeline_event(event_type: str, source_topic: str, payload: dict[str, Any]) -> None:
    try:
        with psycopg2.connect(POSTGRES_APP_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_events (event_type, source_topic, payload) VALUES (%s, %s, %s)",
                (event_type, source_topic, json.dumps(payload)),
            )
            conn.commit()
    except psycopg2.Error as exc:
        logger.error("Failed to write pipeline_event (%s): %s", event_type, exc)


def route_violation(contract: dict[str, Any], record: dict[str, Any], violations: list[str]) -> None:
    on_violation = contract["contract"].get("on_violation", {})
    action = on_violation.get("action", "dead_letter_queue")
    topics = contract["contract"]["kafka"]
    target_topic = topics.get("dlq_topic", DLQ_TOPIC_DEFAULT)
    routed_via = "dead_letter_queue"

    if action == "route_to_healer":
        target_topic = "schema-evolution-requests"
        routed_via = "schema_evolution_requests"

    envelope = {
        "contract_name": contract["contract"]["name"],
        "violations": violations,
        "original_record": record,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
    }
    get_producer().send(target_topic, value=envelope)
    log_pipeline_event(
        "validation_fail",
        topics["source_topic"],
        {"contract": contract["contract"]["name"], "violations": violations, "routed_via": routed_via},
    )
    logger.info("Quarantined record for contract '%s' -> %s (%s)", contract["contract"]["name"], target_topic, violations)


def process_record(contract: dict[str, Any], record: dict[str, Any]) -> None:
    contract_def = contract["contract"]
    fields = contract["schema"]["fields"]

    violations = validate_syntax(record, fields)
    violations += validate_quality(record, contract.get("quality_rules", []))

    if violations:
        route_violation(contract, record, violations)
        return

    masked = mask_pii(record, fields)
    get_producer().send(contract_def["kafka"]["sink_topic"], value=masked)
    log_pipeline_event("validation_pass", contract_def["kafka"]["source_topic"], {"contract": contract_def["name"]})

    fp, shape = schema_fingerprint(fields)
    if _last_fingerprint.get(contract_def["name"]) != fp:
        _last_fingerprint[contract_def["name"]] = fp
        get_producer().send(SCHEMA_EVENTS_TOPIC, value={"dataset_name": contract_def["name"], "schema": shape})
        logger.info("Schema fingerprint changed for '%s' -> emitted schema-event", contract_def["name"])


def consume_topic(source_topic: str, contract: dict[str, Any]) -> None:
    consumer = KafkaConsumer(
        source_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: v,  # decode manually so bad JSON is a violation, not a crash
        auto_offset_reset="earliest",
        group_id="sidecar-validator",
    )
    logger.info("Listening on '%s' for contract '%s'", source_topic, contract["contract"]["name"])
    for message in consumer:
        try:
            record = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            route_violation(contract, {"_raw": str(message.value)[:200]}, ["malformed_json"])
            continue
        try:
            process_record(contract, record)
        except Exception as exc:  # noqa: BLE001 — never let one bad record kill the consumer thread
            logger.exception("Unhandled error validating record on '%s': %s", source_topic, exc)
            route_violation(contract, record, [f"internal_error:{type(exc).__name__}"])


@app.on_event("startup")
def startup() -> None:
    global _contracts
    _contracts = load_contracts(CONTRACTS_DIR)
    if not _contracts:
        logger.warning("No contracts loaded from %s — validator is idle", CONTRACTS_DIR)
    for source_topic, contract in _contracts.items():
        thread = threading.Thread(target=consume_topic, args=(source_topic, contract), daemon=True)
        thread.start()
        time.sleep(0.2)  # stagger consumer-group joins


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "contracts_loaded": len(_contracts)}


@app.get("/contracts")
def list_contracts() -> list[dict[str, Any]]:
    return [
        {
            "name": c["contract"]["name"],
            "version": c["contract"]["version"],
            "source_topic": topic,
            "sink_topic": c["contract"]["kafka"]["sink_topic"],
        }
        for topic, c in _contracts.items()
    ]
