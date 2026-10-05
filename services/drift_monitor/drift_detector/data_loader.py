"""
drift_detector/data_loader.py

Pulls the reference and current windows for drift evaluation out of Postgres.
Kept deliberately separate from the Evidently/statistics code so the query
logic can be unit-tested against a real table without importing Evidently.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Tuple

import pandas as pd
import psycopg2

from drift_detector.config import DriftMonitorConfig

logger = logging.getLogger(__name__)


@dataclass
class WindowBounds:
    reference_start: dt.datetime
    reference_end: dt.datetime
    current_start: dt.datetime
    current_end: dt.datetime


def _resolve_reference_bounds(config: DriftMonitorConfig, now: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    rw = config.reference_window
    if rw.strategy == "fixed":
        return rw.fixed_start, rw.fixed_end  # non-null, enforced in config.py

    ref_end = now - dt.timedelta(days=rw.offset_days)
    ref_start = ref_end - dt.timedelta(days=rw.lookback_days)
    return ref_start, ref_end


def _resolve_current_bounds(config: DriftMonitorConfig, now: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    return now - dt.timedelta(hours=config.current_window.lookback_hours), now


def _fetch_window(dsn: str, config: DriftMonitorConfig, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    cols = config.all_columns
    ts_col = config.source.timestamp_column
    query = f"""
        SELECT {", ".join(cols)}
        FROM {config.source.table}
        WHERE {ts_col} >= %(start)s AND {ts_col} < %(end)s
    """
    # Deliberately not pandas.read_sql(conn=psycopg2 connection) — pandas only
    # formally supports SQLAlchemy engines/connections or DBAPI sqlite3, and
    # warns on a raw psycopg2 connection. Fetching via a cursor avoids the
    # warning without pulling in sqlalchemy for a single query.
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"start": start, "end": end})
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=cols)
    logger.info("Fetched %d rows from %s between %s and %s", len(df), config.source.table, start, end)
    return df


def load_reference_and_current(
    dsn: str,
    config: DriftMonitorConfig,
    now: dt.datetime | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, WindowBounds]:
    """
    Returns (reference_df, current_df, bounds).

    Guards against the two most common ways this silently produces garbage
    results: an undersized reference window, and an empty/near-empty current
    window (e.g., right after a Kafka or sink outage). Both raise loudly
    instead of letting a drift check run on 3 rows and report a false
    "all clear" — the same silent-failure class of bug this project already
    hit once with the Isolation Forest persistence path.
    """
    now = now or dt.datetime.utcnow()

    ref_start, ref_end = _resolve_reference_bounds(config, now)
    cur_start, cur_end = _resolve_current_bounds(config, now)

    reference_df = _fetch_window(dsn, config, ref_start, ref_end)
    current_df = _fetch_window(dsn, config, cur_start, cur_end)

    dd = config.drift_detection
    if len(reference_df) < dd.min_reference_rows:
        raise ValueError(
            f"Reference window has only {len(reference_df)} rows "
            f"(minimum {dd.min_reference_rows}). Refusing to evaluate drift "
            f"against an undersized baseline."
        )
    if len(current_df) < dd.min_current_rows:
        raise ValueError(
            f"Current window has only {len(current_df)} rows "
            f"(minimum {dd.min_current_rows}). This usually means the "
            f"Kafka -> Postgres sink is stalled, not that traffic is quiet — "
            f"treat as an ingestion problem, not 'no drift'."
        )

    bounds = WindowBounds(
        reference_start=ref_start, reference_end=ref_end,
        current_start=cur_start, current_end=cur_end,
    )
    return reference_df, current_df, bounds
