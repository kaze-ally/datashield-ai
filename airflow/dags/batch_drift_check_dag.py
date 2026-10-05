"""
airflow/dags/batch_drift_check_dag.py

Week 3 — runs the real Drift Monitor /check endpoint on a schedule.

All drift-detection logic lives in services/drift_monitor/ (its own
Dockerized FastAPI service, matching sidecar_validator / llm_gateway /
ai_catalog). This DAG is intentionally thin: one HTTP call, fail/retry loudly
on error, and surface the result in Airflow logs and XComs. The scheduler
invokes the live service instead of duplicating the detection logic inside the
Airflow container.
"""
from __future__ import annotations

import datetime as dt
import logging
import os

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from openlineage_events import emit_openlineage_event

logger = logging.getLogger(__name__)

DRIFT_MONITOR_URL = os.environ.get("DRIFT_MONITOR_URL", "http://drift_monitor:8000")
REQUEST_TIMEOUT_SECONDS = 120


def _trigger_drift_check(**context) -> None:
    response = requests.post(f"{DRIFT_MONITOR_URL}/check", timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()  # 422 (undersized window) or 5xx both fail the task, on purpose

    result = response.json()
    logger.info("Drift check result: %s", result)

    context["ti"].xcom_push(key="report_id", value=result.get("report_id"))
    context["ti"].xcom_push(key="dataset_drift_detected", value=result.get("dataset_drift_detected"))

    if result.get("dataset_drift_detected"):
        logger.warning(
            "Dataset drift detected (report_id=%s, drift_share=%.3f) — "
            "drift-events published by the drift_monitor service.",
            result.get("report_id"), result.get("drift_share", 0.0),
        )


default_args = {
    "owner": "datashield-ai",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    dag_id="batch_drift_check",
    schedule_interval="@hourly",
    start_date=dt.datetime(2026, 8, 24),
    catchup=False,
    default_args=default_args,
    tags=["datashield", "drift-detection", "week3"],
) as dag:

    trigger_drift_check = PythonOperator(
        task_id="trigger_drift_check",
        python_callable=_trigger_drift_check,
    )

    publish_lineage = PythonOperator(
        task_id="publish_openlineage_event",
        python_callable=emit_openlineage_event,
        op_kwargs={
            "inputs": [{"namespace": "datashield", "name": "orders_v1"}],
            "outputs": [{"namespace": "datashield", "name": "drift-events"}],
        },
    )

    trigger_drift_check >> publish_lineage
