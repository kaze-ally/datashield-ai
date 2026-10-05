from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from services.ml_isolation_forest import main as isolation_forest


def test_retrain_falls_back_to_validated_orders_when_feature_rows_are_empty():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    timestamp = datetime(2026, 10, 5, 14, 30, tzinfo=timezone.utc)
    cursor.fetchall.side_effect = [[], [(125.5, timestamp)] * 50]

    with (
        patch.object(isolation_forest.psycopg2, "connect", return_value=connection),
        patch.object(isolation_forest, "MIN_BOOTSTRAP_SAMPLES", 50),
        patch.object(isolation_forest, "RETRAIN_WINDOW_SIZE", 2000),
        patch.object(isolation_forest, "fit_from_window", return_value=object()) as fit,
        patch.object(isolation_forest.state, "set_fitted") as set_fitted,
        patch.object(isolation_forest, "log_pipeline_event") as log_event,
        patch.object(isolation_forest.state, "trained_at", "trained-at"),
    ):
        result = isolation_forest.retrain_from_postgres()

    assert result == {
        "status": "retrained",
        "n_samples": 50,
        "source": "validated_orders",
        "trained_at": "trained-at",
    }
    expected_features = [125.5, 14.0]
    fit.assert_called_once_with([expected_features] * 50)
    set_fitted.assert_called_once_with(fit.return_value, 50)
    log_event.assert_called_once_with(
        "model_retrain",
        {"n_samples": 50, "source": "validated_orders", "trained_at": "trained-at"},
    )


def test_live_order_timestamp_uses_same_feature_shape_as_training():
    features = isolation_forest.extract_features({
        "order_total": 125.5,
        "order_timestamp": "2026-10-05T14:30:00+00:00",
        "currency": "USD",
    })

    assert features == [125.5, 14.0]
