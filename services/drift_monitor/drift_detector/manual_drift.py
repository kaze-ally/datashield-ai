"""
drift_detector/manual_drift.py

A small, dependency-light statistical drift check that runs *independently*
of Evidently. This is deliberate defense-in-depth, not duplicated effort:
Evidently's internal Report/Snapshot schema has changed shape across major
versions before, and a silent schema mismatch in evidently_runner.py would
mean the drift-events publish downstream never fires — with no error, no
alert, nothing. This module computes the actual automated decision from
first principles (PSI for numerical columns, chi-squared for categorical),
using only pandas/numpy/scipy, so it can't be broken by an unrelated library
upgrade. Evidently's report remains the rich, human-facing diagnostic
artifact for the dashboard.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from drift_detector.config import DriftMonitorConfig


@dataclass
class ColumnDriftResult:
    column: str
    kind: str      # "numerical" | "categorical"
    method: str    # "psi" | "chi2"
    score: float   # PSI value, or (1 - p_value) for categorical
    drifted: bool


@dataclass
class DriftVerdict:
    dataset_drift: bool
    drift_share: float
    columns: List[ColumnDriftResult] = field(default_factory=list)

    def as_dict(self) -> Dict:
        return {
            "dataset_drift": self.dataset_drift,
            "drift_share": self.drift_share,
            "columns": [c.__dict__ for c in self.columns],
        }


def _psi(reference: pd.Series, current: pd.Series, buckets: int = 10) -> float:
    """
    Standard Population Stability Index over quantile bins of the reference
    distribution. Interpretation (industry-standard, matches Evidently's
    default PSI framing): < 0.1 no significant shift, 0.1-0.25 moderate,
    > 0.25 significant.
    """
    reference = reference.dropna()
    current = current.dropna()
    if reference.empty or current.empty:
        return 0.0

    edges = np.unique(np.quantile(reference, np.linspace(0, 1, buckets + 1)))
    if len(edges) < 3:
        # Near-constant reference column — fall back to a coarse 2-bin split
        edges = np.array([reference.min() - 1e-9, reference.median(), reference.max() + 1e-9])

    ref_counts, _ = np.histogram(reference, bins=edges)
    cur_counts, _ = np.histogram(current, bins=edges)

    ref_pct = np.clip(ref_counts / max(ref_counts.sum(), 1), 1e-4, None)
    cur_pct = np.clip(cur_counts / max(cur_counts.sum(), 1), 1e-4, None)

    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def _chi2_drift(reference: pd.Series, current: pd.Series, alpha: float) -> Tuple[float, bool]:
    """
    Chi-squared goodness-of-fit test comparing category frequencies.
    A category present in only one window is treated as zero-count in the
    other — intentional: a brand-new payment_method or shipping_country
    appearing in `current` IS drift, not noise to be smoothed away.
    Returns (score, drifted) where score = 1 - p_value for dashboard display.
    """
    reference = reference.dropna()
    current = current.dropna()
    ref_counts = reference.value_counts()
    cur_counts = current.value_counts()
    categories = sorted(set(ref_counts.index) | set(cur_counts.index))
    if len(categories) < 2 or current.empty:
        return 0.0, False

    ref_freq = np.array([ref_counts.get(c, 0) for c in categories]) + 1  # Laplace smoothing
    cur_freq = np.array([cur_counts.get(c, 0) for c in categories]) + 1
    ref_freq = ref_freq / ref_freq.sum() * cur_freq.sum()  # rescale to current's total

    _, p_value = stats.chisquare(f_obs=cur_freq, f_exp=ref_freq)
    return float(1.0 - p_value), bool(p_value < alpha)


def compute_manual_drift_verdict(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    config: DriftMonitorConfig,
) -> DriftVerdict:
    dd = config.drift_detection
    results: List[ColumnDriftResult] = []

    for col in config.columns.numerical:
        if col not in reference_df.columns or col not in current_df.columns:
            continue
        score = _psi(reference_df[col], current_df[col])
        results.append(ColumnDriftResult(
            column=col, kind="numerical", method="psi",
            score=round(score, 4), drifted=score >= dd.column_drift_threshold,
        ))

    for col in config.columns.categorical:
        if col not in reference_df.columns or col not in current_df.columns:
            continue
        score, drifted = _chi2_drift(reference_df[col], current_df[col], dd.categorical_significance_level)
        results.append(ColumnDriftResult(
            column=col, kind="categorical", method="chi2",
            score=round(score, 4), drifted=drifted,
        ))

    if not results:
        return DriftVerdict(dataset_drift=False, drift_share=0.0, columns=[])

    drifted_count = sum(1 for r in results if r.drifted)
    drift_share = drifted_count / len(results)
    dataset_drift = drift_share >= dd.dataset_drift_share_threshold

    return DriftVerdict(dataset_drift=dataset_drift, drift_share=round(drift_share, 4), columns=results)
