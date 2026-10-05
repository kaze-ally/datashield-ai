"""
drift_detector/evidently_runner.py

Generates the rich, human-readable Evidently report (HTML + JSON) for the
Streamlit dashboard and audit trail. This module is intentionally NOT the
source of truth for the automated drift decision — see manual_drift.py for
why. If Evidently's API shape changes underneath us on a version bump, this
should degrade to "no report today" and log loudly, not silently corrupt or
block the automated response.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def run_evidently_report(
    current_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    method: str,
    html_output_dir: str,
    report_filename: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (evidently_report_dict, html_path). Either may be None if
    Evidently fails for any reason — this function must never raise into
    the request handler; the diagnostics report is a nice-to-have, not the
    drift gate.
    """
    try:
        from evidently import Report
        from evidently.presets import DataDriftPreset
    except ImportError as exc:
        logger.error("Evidently is not importable (%s) — skipping rich report this run.", exc)
        return None, None

    try:
        report = Report([DataDriftPreset(method=method)], include_tests=True)
        # Evidently's convention: report.run(current_data, reference_data)
        result = report.run(current_df, reference_df)

        report_dict = result.dict()

        Path(html_output_dir).mkdir(parents=True, exist_ok=True)
        html_path = str(Path(html_output_dir) / report_filename)
        result.save_html(html_path)

        return report_dict, html_path

    except Exception:
        logger.exception(
            "Evidently report generation failed — continuing with the "
            "manual drift verdict only. If this persists after a library "
            "upgrade, check the DataDriftPreset/Report signature against "
            "the installed evidently version."
        )
        return None, None
