import random
import statistics

from generate_order_stream import make_record


def test_normal_stream_matches_seeded_reference_distribution():
    random.seed(42)
    records = [make_record() for _ in range(1500)]

    assert abs(statistics.mean(row["order_total"] for row in records) - 50) < 1.5
    assert abs(statistics.mean(row["item_count"] for row in records) - 3) < 0.15
    assert abs(statistics.mean(row["shipping_cost"] for row in records) - 5) < 0.15
    assert abs(statistics.mean(row["discount_pct"] for row in records) - 0.05) < 0.006

    for field, expected in (
        ("payment_method", {"card": 0.7, "paypal": 0.25, "cod": 0.05}),
        ("shipping_country", {"IN": 0.8, "US": 0.15, "UK": 0.05}),
        ("product_category", {"electronics": 0.4, "apparel": 0.4, "home": 0.2}),
    ):
        observed = {
            value: sum(row[field] == value for row in records) / len(records)
            for value in expected
        }
        assert all(abs(observed[value] - probability) < 0.05 for value, probability in expected.items())


def test_drift_shift_moves_order_total_distribution():
    random.seed(7)
    records = [make_record(drift_shift=1) for _ in range(500)]

    assert abs(statistics.mean(row["order_total"] for row in records) - 140) < 2
