"""
drift_detector/config.py

Loads and validates the declarative YAML configuration that drives the
drift monitor service. Same "config as data contract" philosophy as the
sidecar validator's YAML contracts: engineers edit YAML, not service code,
to change what gets monitored.

A malformed config raises pydantic.ValidationError at service startup, on
purpose — a bad config should crash the container and fail health checks,
not silently monitor the wrong columns.

Note: scheduling now lives in airflow/dags/batch_drift_check_dag.py, not
here — this service doesn't know or care how often it gets called, it just
answers each /check request. There's no ScheduleConfig in this file.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, Field, model_validator


class SourceConfig(BaseModel):
    table: str
    timestamp_column: str


class ReferenceWindowConfig(BaseModel):
    strategy: Literal["rolling", "fixed"] = "rolling"
    lookback_days: int = 14
    offset_days: int = 7
    fixed_start: Optional[dt.datetime] = None
    fixed_end: Optional[dt.datetime] = None

    @model_validator(mode="after")
    def _validate_fixed_window(self) -> "ReferenceWindowConfig":
        if self.strategy == "fixed" and (self.fixed_start is None or self.fixed_end is None):
            raise ValueError(
                "reference_window.strategy is 'fixed' but fixed_start/fixed_end are missing"
            )
        return self


class CurrentWindowConfig(BaseModel):
    lookback_hours: float = 1.0


class ColumnsConfig(BaseModel):
    numerical: List[str] = Field(default_factory=list)
    categorical: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_at_least_one_column(self) -> "ColumnsConfig":
        if not self.numerical and not self.categorical:
            raise ValueError("At least one numerical or categorical column must be configured")
        return self


class DriftDetectionConfig(BaseModel):
    method: Literal["psi", "ks", "wasserstein", "jensenshannon"] = "psi"
    column_drift_threshold: float = 0.1
    categorical_significance_level: float = 0.05
    dataset_drift_share_threshold: float = 0.5
    min_reference_rows: int = 200
    min_current_rows: int = 30


class OutputsConfig(BaseModel):
    postgres_table: str = "drift_reports"
    html_report_dir: str = "/app/reports/drift"
    kafka_drift_topic: str = "drift-events"


class AlertingConfig(BaseModel):
    """Controls how often a persisting drift episode is re-announced on Kafka (see publish_gate.py)."""
    renotify_minutes: float = 30.0
    min_publish_interval_seconds: float = 120.0


class DriftMonitorConfig(BaseModel):
    monitor_name: str
    description: str = ""
    source: SourceConfig
    reference_window: ReferenceWindowConfig = ReferenceWindowConfig()
    current_window: CurrentWindowConfig = CurrentWindowConfig()
    columns: ColumnsConfig
    drift_detection: DriftDetectionConfig = DriftDetectionConfig()
    outputs: OutputsConfig = OutputsConfig()
    alerting: AlertingConfig = AlertingConfig()

    @property
    def all_columns(self) -> List[str]:
        return [*self.columns.numerical, *self.columns.categorical]


def load_drift_config(path: Union[str, Path]) -> DriftMonitorConfig:
    """Load and validate a drift-monitor YAML config from disk."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Drift config not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return DriftMonitorConfig.model_validate(raw)
