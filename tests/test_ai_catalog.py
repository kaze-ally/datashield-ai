import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from services.ai_catalog import main as catalog_main


def test_enrich_with_llm_allows_slow_generation_and_parses_tags():
    response = MagicMock()
    response.json.return_value = {
        "response": "Order payments dataset.\nTags: orders, ecommerce, payments"
    }

    with patch.object(catalog_main.requests, "post", return_value=response) as post:
        description, tags = catalog_main.enrich_with_llm(
            "orders_v1", {"order_total": "number"}
        )

    assert description == "Order payments dataset."
    assert tags == ["orders", "ecommerce", "payments"]
    assert post.call_args.kwargs["timeout"] == (5, 120)


def test_enrich_with_llm_logs_gateway_timeout_and_leaves_description_empty(caplog):
    with patch.object(
        catalog_main.requests, "post", side_effect=requests.Timeout("read timed out")
    ):
        description, tags = catalog_main.enrich_with_llm(
            "orders_v1", {"order_total": "number"}
        )

    assert description is None
    assert tags == []
    assert "LLM Gateway request failed while enriching orders_v1" in caplog.text


def test_catalog_upsert_retries_failed_enrichment_without_overwriting_good_text():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value

    with (
        patch.object(catalog_main, "enrich_with_llm", return_value=(None, [])),
        patch.object(catalog_main, "get_db_conn", return_value=connection),
    ):
        catalog_main.upsert_catalog_entry("orders_v1", {"order_total": "number"})

    query, values = cursor.execute.call_args.args
    assert "WHEN EXCLUDED.ai_description IS NOT NULL THEN EXCLUDED.ai_description" in query
    assert "ai_description LIKE '(description pending%%'" in query
    assert values[0] == "orders_v1"
    assert values[3:] == (None, [])
    connection.commit.assert_called_once()


def test_persist_with_retry_succeeds_after_transient_errors():
    attempts = {"count": 0}

    def fake_persist(_event):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise catalog_main.psycopg2.OperationalError("transient database glitch")

    with patch.object(catalog_main.time, "sleep") as sleep:
        assert catalog_main.persist_with_retry({"dataset_name": "orders_v1"}, fake_persist) is True

    assert attempts["count"] == 3
    assert sleep.call_count == 2
    assert sleep.call_args_list[0].args == (1.5,)
    assert sleep.call_args_list[1].args == (3.0,)


def test_persist_with_retry_writes_dlq_and_returns_false_after_exhaustion():
    def fake_persist(_event):
        raise catalog_main.psycopg2.OperationalError("permanent failure")

    with (
        patch.object(catalog_main, "write_to_dlq") as write_to_dlq,
        patch.object(catalog_main.time, "sleep") as sleep,
    ):
        assert catalog_main.persist_with_retry({"dataset_name": "orders_v1"}, fake_persist) is False

    assert write_to_dlq.call_count == 1
    assert sleep.call_count == 3
    assert write_to_dlq.call_args.kwargs["reason"] == "permanent failure"


def test_persist_with_retry_dead_letters_non_database_failure_without_retry():
    def fake_persist(_event):
        raise ValueError("unsupported schema")

    with (
        patch.object(catalog_main, "write_to_dlq") as write_to_dlq,
        patch.object(catalog_main.time, "sleep") as sleep,
    ):
        assert catalog_main.persist_with_retry({"dataset_name": "orders_v1"}, fake_persist) is False

    sleep.assert_not_called()
    write_to_dlq.assert_called_once()
    assert write_to_dlq.call_args.kwargs["reason"] == "unsupported schema"


def test_write_to_dlq_persists_event_payload_and_reason():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    event = {"dataset_name": "orders_v1", "schema": {"order_total": "number"}}

    with patch.object(catalog_main, "get_db_conn", return_value=connection):
        catalog_main.write_to_dlq(event, reason="permanent failure")

    query, values = connection.cursor.return_value.__enter__.return_value.execute.call_args.args
    assert "INSERT INTO ai_catalog_dlq" in query
    assert values == (json.dumps(event), "permanent failure")
    connection.commit.assert_called_once()


def test_consumer_dead_letters_permanent_failure_and_processes_next_message():
    class StopAfterDrain:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, _timeout):
            self.stopped = True
            return True

    broken_message = MagicMock()
    broken_message.value = b'{"dataset_name":"broken","schema":{"field":"number"}}'
    broken_message.partition = 0
    broken_message.offset = 12
    good_message = MagicMock()
    good_message.value = b'{"dataset_name":"orders_v1","schema":{"order_total":"number"}}'
    good_message.partition = 0
    good_message.offset = 13
    consumer = MagicMock()
    consumer.__iter__.return_value = iter([broken_message, good_message])
    persisted = []

    def fake_upsert(dataset_name, schema):
        if dataset_name == "broken":
            raise catalog_main.psycopg2.OperationalError("permanent failure")
        persisted.append((dataset_name, schema))

    stop_event = StopAfterDrain()

    with (
        patch.object(catalog_main, "KafkaConsumer", return_value=consumer) as kafka_consumer,
        patch.object(catalog_main, "upsert_catalog_entry", side_effect=fake_upsert),
        patch.object(catalog_main, "write_to_dlq") as write_to_dlq,
        patch.object(catalog_main.time, "sleep"),
    ):
        catalog_main.consume_loop(stop_event)

    assert persisted == [("orders_v1", {"order_total": "number"})]
    write_to_dlq.assert_called_once_with(
        {"dataset_name": "broken", "schema": {"field": "number"}},
        reason="permanent failure",
    )
    assert consumer.commit.call_count == 2
    consumer.close.assert_called_once_with()
    assert kafka_consumer.call_args.kwargs["auto_offset_reset"] == "earliest"


def test_consumer_does_not_commit_when_dead_letter_write_fails():
    class StopAfterReconnect:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, _timeout):
            self.stopped = True
            return True

    message = MagicMock()
    message.value = b'{"dataset_name":"orders_v1","schema":{"order_total":"number"}}'
    message.partition = 0
    message.offset = 12
    consumer = MagicMock()
    consumer.__iter__.return_value = iter([message])

    with (
        patch.object(catalog_main, "KafkaConsumer", return_value=consumer),
        patch.object(
            catalog_main,
            "upsert_catalog_entry",
            side_effect=catalog_main.psycopg2.OperationalError("database unavailable"),
        ),
        patch.object(
            catalog_main,
            "write_to_dlq",
            side_effect=catalog_main.psycopg2.OperationalError("DLQ unavailable"),
        ),
        patch.object(catalog_main.time, "sleep"),
    ):
        catalog_main.consume_loop(StopAfterReconnect())

    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()


def test_consumer_persists_openlineage_events():
    class StopAfterDrain:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, _timeout):
            self.stopped = True
            return True

    message = MagicMock()
    message.topic = catalog_main.LINEAGE_TOPIC
    message.value = json.dumps(
        {
            "eventType": "COMPLETE",
            "inputs": [{"namespace": "datashield", "name": "raw-data"}],
            "outputs": [{"namespace": "datashield", "name": "orders_v1"}],
        }
    ).encode()
    consumer = MagicMock()
    consumer.__iter__.return_value = iter([message])

    with (
        patch.object(catalog_main, "KafkaConsumer", return_value=consumer) as kafka_consumer,
        patch.object(catalog_main, "persist_lineage_event") as persist_lineage,
    ):
        catalog_main.consume_loop(StopAfterDrain())

    assert catalog_main.LINEAGE_TOPIC in kafka_consumer.call_args.args
    persist_lineage.assert_called_once()
    assert persist_lineage.call_args.args[0]["eventType"] == "COMPLETE"
    consumer.commit.assert_called_once_with()
    consumer.close.assert_called_once_with()


def test_consumer_ignores_openlineage_start_events_without_dead_lettering():
    class StopAfterDrain:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, _timeout):
            self.stopped = True
            return True

    message = MagicMock()
    message.topic = catalog_main.LINEAGE_TOPIC
    message.value = b'{"eventType":"START","run":{"runId":"test"}}'
    consumer = MagicMock()
    consumer.__iter__.return_value = iter([message])

    with (
        patch.object(catalog_main, "KafkaConsumer", return_value=consumer),
        patch.object(catalog_main, "persist_lineage_event") as persist_lineage,
        patch.object(catalog_main, "write_to_dlq") as write_to_dlq,
    ):
        catalog_main.consume_loop(StopAfterDrain())

    persist_lineage.assert_not_called()
    write_to_dlq.assert_not_called()
    consumer.commit.assert_called_once_with()


def test_consumer_dead_letters_invalid_schema_before_committing():
    class StopAfterDrain:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, _timeout):
            self.stopped = True
            return True

    message = MagicMock()
    message.value = b'{"dataset_name":"orders_v1","schema":{}}'
    message.partition = 0
    message.offset = 12
    consumer = MagicMock()
    consumer.__iter__.return_value = iter([message])

    with (
        patch.object(catalog_main, "KafkaConsumer", return_value=consumer),
        patch.object(catalog_main, "write_to_dlq") as write_to_dlq,
        patch.object(catalog_main, "upsert_catalog_entry") as upsert,
    ):
        catalog_main.consume_loop(StopAfterDrain())

    write_to_dlq.assert_called_once_with(
        {"dataset_name": "orders_v1", "schema": {}},
        reason="schema event must contain a non-empty schema object",
    )
    upsert.assert_not_called()
    consumer.commit.assert_called_once_with()


def test_consumer_dead_letters_invalid_json_before_committing():
    class StopAfterDrain:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, _timeout):
            self.stopped = True
            return True

    message = MagicMock()
    message.value = b"\xff"
    message.partition = 0
    message.offset = 12
    consumer = MagicMock()
    consumer.__iter__.return_value = iter([message])

    with (
        patch.object(catalog_main, "KafkaConsumer", return_value=consumer),
        patch.object(catalog_main, "write_to_dlq") as write_to_dlq,
    ):
        catalog_main.consume_loop(StopAfterDrain())

    write_to_dlq.assert_called_once()
    event = write_to_dlq.call_args.args[0]
    reason = write_to_dlq.call_args.kwargs["reason"]
    assert event == {"raw_value": "\ufffd"}
    assert reason.startswith("invalid JSON catalog event:")
    consumer.commit.assert_called_once_with()


def test_catalog_health_reports_consumer_not_started_as_degraded():
    with patch.object(catalog_main, "_consumer_thread", None):
        with patch.object(catalog_main, "get_db_conn") as get_db_conn:
            conn = MagicMock()
            conn.__enter__.return_value = conn
            conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (7,)
            get_db_conn.return_value = conn
            assert catalog_main.health() == {
                "status": "degraded",
                "consumer_alive": False,
                "schema_topic": catalog_main.SCHEMA_TOPIC,
                "lineage_topic": catalog_main.LINEAGE_TOPIC,
                "dlq_count": 7,
            }


def test_catalog_health_surfaces_dlq_count_database_failure():
    with patch.object(catalog_main, "get_db_conn", side_effect=RuntimeError("database down")):
        with pytest.raises(catalog_main.HTTPException) as exc_info:
            catalog_main.health()

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Unable to read AI Catalog dead-letter queue count"


def test_lineage_dataset_names_ignore_invalid_and_duplicate_entries():
    assert catalog_main._lineage_dataset_names(
        [
            {"namespace": "datashield", "name": "orders_v1"},
            {"name": "orders_v1"},
            {"name": "  "},
            None,
        ]
    ) == ["orders_v1"]


def test_persist_lineage_event_merges_upstream_and_downstream_edges():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value

    event = {
        "eventType": "COMPLETE",
        "inputs": [{"namespace": "datashield", "name": "raw-data"}],
        "outputs": [{"namespace": "datashield", "name": "orders_v1"}],
    }
    with patch.object(catalog_main, "get_db_conn", return_value=connection):
        catalog_main.persist_lineage_event(event)

    assert cursor.execute.call_count == 2
    first_query, first_values = cursor.execute.call_args_list[0].args
    second_query, second_values = cursor.execute.call_args_list[1].args
    assert "lineage_upstream" in first_query
    assert first_values == (["raw-data"], "orders_v1")
    assert "lineage_downstream" in second_query
    assert second_values == (["orders_v1"], "raw-data")
    connection.commit.assert_called_once()


def test_persist_lineage_event_rejects_events_without_datasets():
    with pytest.raises(ValueError, match="at least one named input or output"):
        catalog_main.persist_lineage_event({"inputs": [], "outputs": []})
