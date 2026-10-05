"""
breaker/config.py

Loads and validates the circuit breaker's YAML config — same
config-as-data-contract philosophy as the sidecar validator and drift
monitor. A malformed config crashes the container at startup, on purpose.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Union

import yaml
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    topic: str
    monitor_name_field: str = "monitor_name"
    report_id_field: str = "report_id"


class BreakerTuning(BaseModel):
    failure_threshold: int = 3
    cooldown_seconds: float = 300.0
    cooldown_backoff_multiplier: float = 2.0
    cooldown_max_seconds: float = 3600.0
    cooldown_jitter_seconds: float = 30.0
    dry_run: bool = False
    # Event-admission guards (see breaker/admission.py). 0 disables the age check.
    max_event_age_seconds: float = 900.0          # drop drift events older than this (stale backlog replay)
    blocked_audit_interval_seconds: float = 60.0  # while OPEN, audit/DLQ at most once per interval per monitor


class OutputsConfig(BaseModel):
    remediation_requests_topic: str = "remediation-requests"
    remediation_dlq_topic: str = "remediation-dlq"
    postgres_state_table: str = "circuit_breaker_state"
    postgres_events_table: str = "circuit_breaker_events"


class CircuitBreakerConfig(BaseModel):
    sources: List[SourceConfig] = Field(default_factory=list)
    breaker: BreakerTuning = BreakerTuning()
    outputs: OutputsConfig = OutputsConfig()


def load_breaker_config(path: Union[str, Path]) -> CircuitBreakerConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Circuit breaker config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return CircuitBreakerConfig.model_validate(raw)
