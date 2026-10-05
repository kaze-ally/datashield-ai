"""
scripts/seed_test_orders.py

Seeds validated_orders with synthetic data for testing the drift monitor —
same generator used to validate services/drift_monitor before it shipped.
No new dependencies: reuses whatever's already installed in the
drift_monitor image (pandas, numpy, psycopg2), so run it via that image
rather than installing anything fresh on your host — see TESTING_week3.md.

Usage:
    python scripts/seed_test_orders.py --scenario stable --rows 150
    python scripts/seed_test_orders.py --scenario drift  --rows 150
    python scripts/seed_test_orders.py --reference-only         # seed the 14-day baseline once

Reads DATASHIELD_POSTGRES_DSN from the environment (same variable the
service itself uses).
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

import numpy as np
import pandas as pd
import psycopg2

COLUMNS = [
    "order_total", "item_count", "shipping_cost", "discount_pct",
    "payment_method", "shipping_country", "product_category", "ingested_at",
]


def _stable_distribution(n: int, rng: np.random.Generator, timestamps: list) -> pd.DataFrame:
    return pd.DataFrame({
        "order_total": rng.normal(50, 15, n).clip(1),
        "item_count": rng.poisson(3, n),
        "shipping_cost": rng.normal(5, 1.5, n).clip(0),
        "discount_pct": rng.uniform(0, 0.1, n),
        "payment_method": rng.choice(["card", "paypal", "cod"], n, p=[0.7, 0.25, 0.05]),
        "shipping_country": rng.choice(["IN", "US", "UK"], n, p=[0.8, 0.15, 0.05]),
        "product_category": rng.choice(["electronics", "apparel", "home"], n, p=[0.4, 0.4, 0.2]),
        "ingested_at": timestamps,
    })


def _drifted_distribution(n: int, rng: np.random.Generator, timestamps: list) -> pd.DataFrame:
    return pd.DataFrame({
        "order_total": rng.normal(140, 30, n).clip(1),        # big upward shift
        "item_count": rng.poisson(3, n),
        "shipping_cost": rng.normal(5, 1.5, n).clip(0),
        "discount_pct": rng.uniform(0, 0.1, n),
        "payment_method": rng.choice(["card", "paypal", "cod", "crypto"], n, p=[0.3, 0.2, 0.1, 0.4]),  # new category
        "shipping_country": rng.choice(["IN", "US", "UK"], n, p=[0.2, 0.6, 0.2]),  # skewed hard
        "product_category": rng.choice(["electronics", "apparel", "home"], n, p=[0.4, 0.4, 0.2]),
        "ingested_at": timestamps,
    })


def _insert(dsn: str, df: pd.DataFrame) -> None:
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.executemany(
                f"""INSERT INTO validated_orders ({", ".join(COLUMNS)})
                    VALUES ({", ".join(["%s"] * len(COLUMNS))})""",
                df[COLUMNS].values.tolist(),
            )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=["stable", "drift"], default="stable",
                         help="Distribution shape for the CURRENT window (last hour). Ignored with --reference-only.")
    parser.add_argument("--rows", type=int, default=150, help="Row count for the current-window insert.")
    parser.add_argument("--reference-only", action="store_true",
                         help="Seed a 14-day reference/baseline window instead of the current window.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed (default: time-based).")
    args = parser.parse_args()

    dsn = os.environ.get("DATASHIELD_POSTGRES_DSN")
    if not dsn:
        print("DATASHIELD_POSTGRES_DSN is not set.", file=sys.stderr)
        sys.exit(1)

    rng = np.random.default_rng(args.seed)
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    if args.reference_only:
        ref_end = now - dt.timedelta(days=7)     # matches offset_days: 7 in the config
        ref_start = ref_end - dt.timedelta(days=14)  # matches lookback_days: 14
        n = 3000
        timestamps = [ref_start + (ref_end - ref_start) * rng.random() for _ in range(n)]
        df = _stable_distribution(n, rng, timestamps)
        _insert(dsn, df)
        print(f"Inserted {n} reference-window rows ({ref_start} to {ref_end})")
        return

    timestamps = [now - dt.timedelta(minutes=rng.random() * 59) for _ in range(args.rows)]
    df = _drifted_distribution(args.rows, rng, timestamps) if args.scenario == "drift" \
        else _stable_distribution(args.rows, rng, timestamps)
    _insert(dsn, df)
    print(f"Inserted {args.rows} current-window rows (scenario={args.scenario})")


if __name__ == "__main__":
    main()
