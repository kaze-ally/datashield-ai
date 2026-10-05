"""
tests/test_drift_monitor.py

Unit tests for the independent statistical drift check (manual_drift.py) —
the piece that actually gates the drift-events publish. Run with:
    pytest tests/test_drift_monitor.py -v
"""
import numpy as np
import pandas as pd
import pytest

from drift_detector.config import (
    ColumnsConfig,
    DriftDetectionConfig,
    DriftMonitorConfig,
    SourceConfig,
)
from drift_detector.manual_drift import compute_manual_drift_verdict


@pytest.fixture
def base_config() -> DriftMonitorConfig:
    return DriftMonitorConfig(
        monitor_name="test_monitor",
        source=SourceConfig(table="validated_orders", timestamp_column="ingested_at"),
        columns=ColumnsConfig(
            numerical=["order_total"],
            categorical=["payment_method"],
        ),
        drift_detection=DriftDetectionConfig(
            column_drift_threshold=0.1,
            categorical_significance_level=0.05,
            dataset_drift_share_threshold=0.5,
        ),
    )


def _stable_reference(n: int = 2000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "order_total": rng.normal(50, 15, n).clip(1),
        "payment_method": rng.choice(["card", "paypal", "cod"], n, p=[0.7, 0.25, 0.05]),
    })


def test_no_drift_when_current_matches_reference(base_config):
    reference_df = _stable_reference()
    current_df = reference_df.sample(300, random_state=1).reset_index(drop=True)

    verdict = compute_manual_drift_verdict(reference_df, current_df, base_config)

    assert verdict.dataset_drift is False
    assert verdict.drift_share == 0.0
    assert all(not c.drifted for c in verdict.columns)


def test_drift_flagged_on_shifted_numerical_and_new_category(base_config):
    reference_df = _stable_reference()
    rng = np.random.default_rng(7)
    current_df = pd.DataFrame({
        "order_total": rng.normal(120, 25, 300).clip(1),  # large upward shift
        "payment_method": rng.choice(["card", "paypal", "cod", "crypto"], 300, p=[0.4, 0.2, 0.1, 0.3]),
    })

    verdict = compute_manual_drift_verdict(reference_df, current_df, base_config)

    column_map = {c.column: c for c in verdict.columns}
    assert column_map["order_total"].drifted is True
    assert column_map["payment_method"].drifted is True
    # With both monitored columns drifting, dataset-level drift share is 1.0
    assert verdict.drift_share == 1.0
    assert verdict.dataset_drift is True


def test_two_of_seven_drifted_columns_cross_quarter_share_threshold():
    config = DriftMonitorConfig(
        monitor_name="test_monitor",
        source=SourceConfig(table="validated_orders", timestamp_column="ingested_at"),
        columns=ColumnsConfig(
            numerical=["order_total", "item_count", "shipping_cost", "discount_pct"],
            categorical=["payment_method", "shipping_country", "product_category"],
        ),
        drift_detection=DriftDetectionConfig(
            column_drift_threshold=0.1,
            categorical_significance_level=0.05,
            dataset_drift_share_threshold=0.25,
        ),
    )
    rng = np.random.default_rng(12)
    reference_df = pd.DataFrame({
        "order_total": rng.normal(50, 15, 2000).clip(1),
        "item_count": 3,
        "shipping_cost": 5.0,
        "discount_pct": 0.05,
        "payment_method": np.tile(["card", "paypal", "cod", "card"], 500),
        "shipping_country": np.tile(["IN", "US", "UK", "IN"], 500),
        "product_category": np.tile(["electronics", "apparel", "home", "apparel"], 500),
    })
    current_df = reference_df.iloc[:500].copy()
    current_df["order_total"] = rng.normal(140, 15, 500).clip(1)
    current_df["shipping_country"] = "DE"

    verdict = compute_manual_drift_verdict(reference_df, current_df, config)

    assert sum(column.drifted for column in verdict.columns) == 2
    assert verdict.drift_share >= 0.25
    assert verdict.dataset_drift is True

    config.drift_detection.dataset_drift_share_threshold = 0.5
    assert compute_manual_drift_verdict(reference_df, current_df, config).dataset_drift is False


def test_missing_columns_are_skipped_not_errored(base_config):
    reference_df = pd.DataFrame({"order_total": [1.0, 2.0, 3.0] * 100})  # no payment_method column
    current_df = pd.DataFrame({"order_total": [1.0, 2.0, 3.0] * 20})

    verdict = compute_manual_drift_verdict(reference_df, current_df, base_config)

    assert [c.column for c in verdict.columns] == ["order_total"]


def test_empty_column_config_returns_no_drift(base_config):
    base_config.columns.numerical = []
    base_config.columns.categorical = []
    reference_df = _stable_reference()
    current_df = reference_df.sample(50, random_state=1)

    # ColumnsConfig validator requires >=1 column at construction time, so
    # mutating after construction to simulate "nothing matched" is what
    # compute_manual_drift_verdict guards against directly:
    verdict = compute_manual_drift_verdict(reference_df, current_df, base_config)
    assert verdict.dataset_drift is False
    assert verdict.columns == []
