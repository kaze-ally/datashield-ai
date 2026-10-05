"""
rca/config.py

Config-as-data-contract, same as every other service. Malformed config
crashes the container at startup, on purpose.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Union

import yaml
from pydantic import BaseModel


class SourceConfig(BaseModel):
    topic: str
    monitor_name_field: str = "monitor_name"
    report_id_field: str = "report_id"


class ContextConfig(BaseModel):
    drift_reports_table: str = "drift_reports"
    circuit_breaker_url: str = "http://circuit_breaker:8000"


class LLMConfig(BaseModel):
    base_url: str = "http://ollama:11434"
    model: str = "phi4-mini"
    timeout_seconds: float = 60.0
    fallback_model: str = "llama3.1:8b"


class OutcomePolicyConfig(BaseModel):
    auto_reset_severities: List[str] = ["low", "medium"]
    min_confidence_to_trust_severity: float = 0.5


class OutputsConfig(BaseModel):
    postgres_table: str = "rca_reports"


class RCAConfig(BaseModel):
    source: SourceConfig
    context: ContextConfig = ContextConfig()
    llm: LLMConfig = LLMConfig()
    outcome_policy: OutcomePolicyConfig = OutcomePolicyConfig()
    outputs: OutputsConfig = OutputsConfig()


def load_rca_config(path: Union[str, Path]) -> RCAConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"RCA config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return RCAConfig.model_validate(raw)
