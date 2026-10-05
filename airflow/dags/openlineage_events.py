"""Small Kafka transport for OpenLineage-compatible Airflow run events."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any


def emit_openlineage_event(*, inputs: list[dict[str, str]], outputs: list[dict[str, str]], **context: Any) -> None:
    """Publish a successful Airflow task's dataset edges to the catalog topic."""
    from kafka import KafkaProducer

    dag_run = context.get("dag_run")
    task_instance = context.get("ti")
    dag_id = getattr(getattr(task_instance, "dag", None), "dag_id", None)
    dag_id = dag_id or getattr(getattr(dag_run, "dag", None), "dag_id", "datashield_pipeline")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id") or "manual__unknown"
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": str(run_id)},
        "job": {"namespace": "datashield", "name": dag_id},
        "inputs": inputs,
        "outputs": outputs,
    }
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        acks="all",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        producer.send(os.environ.get("OPENLINEAGE_TOPIC", "openlineage-events"), event).get(timeout=15)
        producer.flush(timeout=15)
    finally:
        producer.close(timeout=10)
