"""
DataShield AI — Expanded Order Stream Generator
-------------------------------------------------
Publishes richer, higher-volume synthetic order events to Kafka's raw-data
topic than the original test seed scripts — more dimensions per record, and
configurable rate/volume so you can push the pipeline for scalability
testing rather than just correctness testing.

New dimensions beyond the original order_id/order_total/payment_method:
customer_id, customer_segment, region, product_category, shipping_country,
item_count, unit_price, discount_pct, shipping_method, shipping_cost,
currency, device_type, order_channel, session_id, is_first_purchase,
loyalty_tier, warehouse_id, order_timestamp.

Because sidecar_validator's strict_extra_fields defaults to false (per
README_week5_schema_healer.md's Addendum 2), these extra fields won't by
themselves break orders_v1 validation — they'll just ride along as
unrecognized-but-tolerated fields until you decide which ones to add to
the contract and to validated_orders' schema (see the ALTER TABLE note at
the bottom of this file).

Usage:
    pip install kafka-python
    python generate_order_stream.py --rate 20 --duration 300
    python generate_order_stream.py --count 5000 --anomaly-rate 0.02 --mismatch-rate 0.05
    python generate_order_stream.py --rate 10 --duration 3600 --drift-after 1800

Env vars:
    DATASHIELD_KAFKA_BOOTSTRAP   default: localhost:29092
    RAW_TOPIC                    default: raw-data
"""
import argparse
import itertools
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

REGIONS = ["NA", "EU", "APAC", "LATAM"]
CATEGORIES = ["electronics", "apparel", "home"]
CATEGORY_WEIGHTS = [0.4, 0.4, 0.2]
SEGMENTS = ["new", "returning", "vip"]
SHIP_METHODS = ["standard", "express", "overnight"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
DEVICES = ["mobile", "desktop", "tablet"]
CHANNELS = ["web", "app", "marketplace"]
LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
PAYMENT_METHODS = ["card", "paypal", "cod"]
PAYMENT_METHOD_WEIGHTS = [0.7, 0.25, 0.05]
SHIPPING_COUNTRIES = ["IN", "US", "UK"]
SHIPPING_COUNTRY_WEIGHTS = [0.8, 0.15, 0.05]
WAREHOUSES = ["WH-EAST-1", "WH-WEST-2", "WH-EU-1", "WH-APAC-1"]

_order_seq = itertools.count(1)


def _sample_poisson(mean: float) -> int:
    limit = math.exp(-mean)
    product = 1.0
    count = 0
    while product > limit:
        count += 1
        product *= random.random()
    return count - 1


def make_record(drift_shift: float = 0.0) -> dict:
    """One synthetic order. `drift_shift` (0-1) nudges price/discount
    distributions upward to simulate real, gradual data drift rather than
    a hard-cut regime change. At zero shift, monitored fields match the
    stable reference distribution produced by scripts/seed_test_orders.py."""
    item_count = _sample_poisson(3)
    order_total = round(max(1.0, random.gauss(50 + drift_shift * 90, 15)), 2)
    unit_price = round(order_total / max(item_count, 1), 2)
    discount_pct = round(min(0.5, random.uniform(0, 0.1) + drift_shift * 0.1), 3)
    shipping_cost = round(max(0.0, random.gauss(5, 1.5)), 2)

    return {
        "order_id": f"ord-{next(_order_seq):07d}-{uuid.uuid4().hex[:6]}",
        "order_total": order_total,
        "payment_method": random.choices(PAYMENT_METHODS, PAYMENT_METHOD_WEIGHTS)[0],
        "customer_id": f"cust-{random.randint(1, 50_000)}",
        "customer_segment": random.choice(SEGMENTS),
        "region": random.choice(REGIONS),
        "product_category": random.choices(CATEGORIES, CATEGORY_WEIGHTS)[0],
        "item_count": item_count,
        "unit_price": unit_price,
        "discount_pct": discount_pct,
        "shipping_method": random.choice(SHIP_METHODS),
        "shipping_cost": shipping_cost,
        "shipping_country": random.choices(SHIPPING_COUNTRIES, SHIPPING_COUNTRY_WEIGHTS)[0],
        "currency": random.choice(CURRENCIES),
        "device_type": random.choice(DEVICES),
        "order_channel": random.choice(CHANNELS),
        "session_id": str(uuid.uuid4()),
        "is_first_purchase": random.random() < 0.18,
        "loyalty_tier": random.choice(LOYALTY_TIERS),
        "warehouse_id": random.choice(WAREHOUSES),
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_anomaly() -> dict:
    """A record shaped like a genuine outlier for the Isolation Forest —
    not malformed, just statistically extreme (matches the 'injected
    high-value outliers' pattern the historical validation reports describe)."""
    rec = make_record()
    rec["order_total"] = round(random.uniform(5000, 25000), 2)
    rec["item_count"] = random.randint(40, 200)
    return rec


def make_schema_mismatch() -> dict:
    """Reproduces the exact real mismatch shape from SCHEMA_HEALER_LIVE_TEST_REPORT.md
    — order_amt instead of order_total — so the schema healer has real work to do."""
    rec = make_record()
    rec["order_amt"] = str(rec.pop("order_total"))
    return rec


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rate", type=float, default=10.0, help="records/sec (default 10)")
    ap.add_argument("--duration", type=int, default=None, help="seconds to run (mutually exclusive with --count)")
    ap.add_argument("--count", type=int, default=None, help="total records to send, then exit")
    ap.add_argument("--anomaly-rate", type=float, default=0.01, help="fraction of records that are extreme outliers")
    ap.add_argument("--mismatch-rate", type=float, default=0.03, help="fraction of records with a schema mismatch")
    ap.add_argument("--drift-after", type=int, default=None, help="seconds after which to start shifting distributions (simulated drift)")
    ap.add_argument("--topic", default=os.environ.get("RAW_TOPIC", "raw-data"))
    ap.add_argument("--bootstrap-servers", default=os.environ.get("DATASHIELD_KAFKA_BOOTSTRAP", "localhost:29092"))
    args = ap.parse_args()

    if args.duration and args.count:
        ap.error("use --duration or --count, not both")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,       # small batching window — meaningfully higher throughput at scale
        batch_size=32_768,
        acks=1,
    )

    start = time.monotonic()
    sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0

    print(f"[generator] streaming to {args.bootstrap_servers} / topic={args.topic} at ~{args.rate}/s")
    try:
        while True:
            if args.count is not None and sent >= args.count:
                break
            elapsed = time.monotonic() - start
            if args.duration is not None and elapsed >= args.duration:
                break

            drift_shift = 0.0
            if args.drift_after is not None and elapsed >= args.drift_after:
                # ramps 0 -> 1 over the 10 minutes after drift kicks in
                drift_shift = min(1.0, (elapsed - args.drift_after) / 600)

            roll = random.random()
            if roll < args.anomaly_rate:
                record = make_anomaly()
            elif roll < args.anomaly_rate + args.mismatch_rate:
                record = make_schema_mismatch()
            else:
                record = make_record(drift_shift)

            producer.send(args.topic, value=record)
            sent += 1
            if sent % 500 == 0:
                print(f"[generator] sent {sent} records (drift_shift={drift_shift:.2f})")

            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[generator] stopping...")
    finally:
        producer.flush()
        producer.close()
        print(f"[generator] done — sent {sent} records in {time.monotonic()-start:.1f}s")


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Optional: to actually persist the new dimensions (rather than have them
# ride along as tolerated-but-unused extra fields), extend validated_orders
# and orders_v1's contract yourself, e.g.:
#
#   ALTER TABLE validated_orders
#     ADD COLUMN IF NOT EXISTS customer_segment TEXT,
#     ADD COLUMN IF NOT EXISTS region TEXT,
#     ADD COLUMN IF NOT EXISTS product_category TEXT,
#     ADD COLUMN IF NOT EXISTS item_count INTEGER,
#     ADD COLUMN IF NOT EXISTS unit_price NUMERIC,
#     ADD COLUMN IF NOT EXISTS discount_pct NUMERIC,
#     ADD COLUMN IF NOT EXISTS device_type TEXT,
#     ADD COLUMN IF NOT EXISTS order_channel TEXT;
#
# Then add the ones you want drift-checked to drift_detection_config.yaml's
# `columns:` list (region/product_category/device_type as categorical
# checks alongside order_total; unit_price/discount_pct as numerical PSI
# checks) — this is what actually turns "more dimensions" into "more
# drift-detection surface," not just wider records.
# ---------------------------------------------------------------------------
