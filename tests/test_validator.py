"""tests/test_validator.py — no infra needed."""
from unittest.mock import MagicMock, patch

from services.schema_healer.healer.contract_loader import DataContract, FieldContract
from services.schema_healer.healer.kafka_io import parse_schema_mismatch_message
from services.schema_healer.healer.validator import validate_record
from services.sidecar_validator import main as sidecar_main

CONTRACT = DataContract(
    contract_name="orders_v1",
    version=1,
    fields=[
        FieldContract(name="order_id", type="string", required=True),
        FieldContract(name="order_total", type="float", required=True),
    ],
)


def test_missing_required_field_is_always_invalid():
    result = validate_record({"order_id": "o1"}, CONTRACT)
    assert result.valid is False
    assert "order_total" in result.missing_required


def test_type_mismatch_is_always_invalid():
    result = validate_record({"order_id": "o1", "order_total": "not-a-number"}, CONTRACT)
    assert result.valid is False
    assert any("order_total" in m for m in result.type_mismatches)


def test_extra_field_is_diagnostic_only_by_default():
    result = validate_record({"order_id": "o1", "order_total": 1.0, "extra": "x"}, CONTRACT)
    assert result.valid is True  # lenient default
    assert "extra" in result.unexpected_fields  # still recorded for diagnostics/prompting


def test_extra_field_blocks_when_strict_extra_fields_true():
    result = validate_record(
        {"order_id": "o1", "order_total": 1.0, "extra": "x"}, CONTRACT, strict_extra_fields=True
    )
    assert result.valid is False
    assert "extra" in result.unexpected_fields


def test_fully_valid_record_passes_either_way():
    record = {"order_id": "o1", "order_total": 1.0}
    assert validate_record(record, CONTRACT, strict_extra_fields=False).valid is True
    assert validate_record(record, CONTRACT, strict_extra_fields=True).valid is True


def test_sidecar_routes_healable_violations_with_schema_healer_envelope():
    contract = {
        "contract": {
            "name": "orders_v1",
            "kafka": {"source_topic": "raw-data"},
        },
        "on_violation": {"action": "route_to_healer"},
    }
    record = {"order_id": "o1", "order_amt": "42.5"}
    producer = MagicMock()
    with (
        patch.object(sidecar_main, "get_producer", return_value=producer),
        patch.object(sidecar_main, "log_pipeline_event"),
    ):
        sidecar_main.route_violation(contract, record, ["missing_required_field:order_total"])

    topic, = producer.send.call_args.args
    envelope = producer.send.call_args.kwargs["value"]
    assert topic == "schema-evolution-requests"
    assert envelope["event_type"] == "schema_mismatch"
    assert envelope["contract_name"] == "orders_v1"
    assert envelope["record"] == record
    assert envelope["report_id"]
    assert parse_schema_mismatch_message(envelope)["record"] == record


def test_schema_healer_ignores_schema_fingerprint_events():
    assert parse_schema_mismatch_message({"dataset_name": "orders_v1", "schema": []}) is None


def test_publish_schema_event_is_idempotent_and_waits_for_delivery():
    contract = {
        "contract": {"name": "orders_v1"},
        "schema": {"fields": [{"name": "order_total", "type": "float"}]},
    }
    producer = MagicMock()
    delivery = producer.send.return_value

    with (
        patch.object(sidecar_main, "_last_fingerprint", {}),
        patch.object(sidecar_main, "get_producer", return_value=producer),
    ):
        sidecar_main.publish_schema_event(contract)
        sidecar_main.publish_schema_event(contract)

    producer.send.assert_called_once_with(
        "schema-events",
        value={"dataset_name": "orders_v1", "schema": {"order_total": "float"}},
    )
    delivery.get.assert_called_once_with(timeout=10)


def test_sidecar_startup_publishes_loaded_contract_schema():
    contract = {"contract": {"name": "orders_v1"}}
    with (
        patch.object(sidecar_main, "load_contracts", return_value={"raw-data": contract}),
        patch.object(sidecar_main, "publish_schema_event") as publish,
        patch.object(sidecar_main.threading, "Thread"),
        patch.object(sidecar_main.time, "sleep"),
    ):
        sidecar_main.startup()

    publish.assert_called_once_with(contract)


def test_validated_orders_sink_maps_stream_fields_and_is_idempotent():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    with (
        patch.object(sidecar_main, "POSTGRES_APP_DSN", "postgresql://test"),
        patch.object(sidecar_main.psycopg2, "connect", return_value=connection) as connect,
    ):
        sidecar_main.persist_validated_order(
            {
                "order_id": "ord-1",
                "order_total": 42.5,
                "item_count": 2,
                "shipping_cost": 4.99,
                "discount_pct": 0.1,
                "payment_method": "credit_card",
                "product_category": "home",
                "region": "EU",
            }
        )

    connect.assert_called_once_with("postgresql://test")
    query, values = cursor.execute.call_args.args
    assert "ON CONFLICT (order_id) DO NOTHING" in query
    assert values == ("ord-1", 42.5, 2, 4.99, 0.1, "credit_card", None, "home")
