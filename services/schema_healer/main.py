"""
services/schema_healer/main.py

FastAPI: /health, /reports/{contract_name}, /guard/{contract_name}.
Same shape as circuit_breaker's and rca_copilot's main.py — a thin HTTP
surface over a background Kafka consumer thread that does the real work.
"""
from __future__ import annotations

import logging
import os
import uuid

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from healer.circuit_guard import CircuitGuardConfig, check as guard_check
from healer.contract_loader import ContractNotFound, ContractStore
from healer.kafka_io import SchemaHealerConsumer, SchemaHealerPublisher, utcnow_iso
from healer.llm_client import OllamaHealerClient
from healer.persistence import SchemaHealerRepository
from healer.pipeline import heal_one_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("schema_healer.main")

CONFIG_PATH = os.environ.get("SCHEMA_HEALER_CONFIG_PATH", "/app/config/schema_healer_config.yaml")
with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)

POSTGRES_DSN = os.environ["DATASHIELD_POSTGRES_DSN"]
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

contracts = ContractStore(CONFIG["contracts"]["dir"])
repo = SchemaHealerRepository(POSTGRES_DSN)
llm_client = OllamaHealerClient(
    base_url=CONFIG["llm"]["base_url"],
    model=CONFIG["llm"]["model"],
    fallback_model=CONFIG["llm"]["fallback_model"],
    timeout_seconds=CONFIG["llm"]["timeout_seconds"],
)
guard_config = CircuitGuardConfig(
    failure_threshold=CONFIG["circuit_guard"]["failure_threshold"],
    base_cooldown_seconds=CONFIG["circuit_guard"]["base_cooldown_seconds"],
    backoff_multiplier=CONFIG["circuit_guard"]["backoff_multiplier"],
    max_cooldown_seconds=CONFIG["circuit_guard"]["max_cooldown_seconds"],
)
publisher = SchemaHealerPublisher(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    validated_topic=CONFIG["kafka"]["validated_topic"],
    dlq_topic=CONFIG["kafka"]["dlq_topic"],
)


def handle_message(normalized: dict) -> None:
    contract_name = normalized["contract_name"]
    record = normalized["record"]
    report_id = normalized["report_id"]

    try:
        contract = contracts.get(contract_name)
    except ContractNotFound:
        logger.error("no contract found for contract_name=%s, sending to DLQ", contract_name)
        publisher.publish_dlq(
            record,
            {"reason": f"contract_not_found: {contract_name}", "report_id": report_id},
        )
        return

    contract_path_candidates = list(contracts.contracts_dir.rglob(f"{contract_name}*.yaml"))
    contract_yaml_text = contract_path_candidates[0].read_text() if contract_path_candidates else ""

    from healer.validator import validate_record

    strict_extra_fields = CONFIG["healing"].get("strict_extra_fields", False)
    diagnostic_text = validate_record(
        record, contract, strict_extra_fields=strict_extra_fields
    ).as_diagnostic_text()
    guard_state = repo.load_guard_state(contract_name)

    outcome = heal_one_record(
        record=record,
        contract=contract,
        contract_yaml_text=contract_yaml_text,
        diagnostic_text=diagnostic_text,
        llm_client=llm_client,
        guard_state=guard_state,
        guard_config=guard_config,
        min_confidence=CONFIG["healing"]["min_confidence"],
        strict_extra_fields=strict_extra_fields,
    )

    repo.save_guard_state(outcome.new_guard_state)
    repo.save_report(report_id, contract_name, outcome)

    lineage = {
        "report_id": report_id,
        "contract_name": contract_name,
        "success": outcome.success,
        "reason": outcome.reason,
        "root_cause_category": outcome.root_cause_category,
        "confidence": outcome.confidence,
        "model_used": outcome.model_used,
        "healed_at": utcnow_iso(),
    }

    if outcome.success:
        publisher.publish_healed(outcome.healed_record, lineage)
        logger.info(
            "healed record for contract=%s report_id=%s confidence=%.2f",
            contract_name,
            report_id,
            outcome.confidence,
        )
    else:
        publisher.publish_dlq(record, lineage)
        logger.warning(
            "could not heal record for contract=%s report_id=%s reason=%s",
            contract_name,
            report_id,
            outcome.reason,
        )


consumer = SchemaHealerConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic=CONFIG["kafka"]["source_topic"],
    group_id=CONFIG["kafka"]["consumer_group"],
    handle_message=handle_message,
)

app = FastAPI(title="schema_healer")

# LOCAL-DASHBOARD FIX (2026-09-24): a browser-based dashboard fetching this
# service's JSON directly (e.g. a locally-run status page) is a
# cross-origin request from the browser's point of view, and FastAPI
# blocks those by default. allow_origins=["*"] is fine for a service that
# only ever runs on localhost for local development — do not carry this
# into a real deployment reachable from the internet.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    consumer.start()


@app.get("/health")
def health():
    return {"status": "ok", "consumer_alive": consumer.consumer_alive}


@app.get("/reports/{contract_name}")
def reports(contract_name: str, limit: int = 10):
    return repo.recent_reports(contract_name, limit=limit)


@app.get("/guard/{contract_name}")
def guard_status(contract_name: str):
    state = repo.load_guard_state(contract_name)
    from healer.circuit_guard import utcnow

    decision = guard_check(state, utcnow())
    return {
        "contract_name": state.contract_name,
        "consecutive_failures": state.consecutive_failures,
        "trip_count": state.trip_count,
        "paused_until": state.paused_until,
        "allowed_now": decision.allowed,
        "reason": decision.reason,
    }


@app.post("/heal-now/{contract_name}")
def heal_now(contract_name: str, record: dict):
    """Manual trigger for testing — mirrors the manual-exercise pattern
    README_week4.md uses for /report-outcome before a real producer exists."""
    report_id = str(uuid.uuid4())
    handle_message({"report_id": report_id, "contract_name": contract_name, "record": record})
    return {"report_id": report_id, "status": "processed"}
