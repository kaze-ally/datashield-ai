"""
DataShield AI — Isolation Forest Retrain DAG 
================================================================
Paper 5's "Trustworthy MLOps" argument is that retraining has to be an
orchestrated, observable part of the pipeline — not a manual notebook run.
This DAG is that orchestration: it calls the streaming detector's own
`/retrain` endpoint, which refits on the rolling window of feature vectors
it's been persisting to Postgres (`order_features`), falling back to
`validated_orders` when the feature table is not yet populated, and hot-swaps
the new model in-process. See services/ml_isolation_forest/main.py for the
actual training logic — this DAG is intentionally thin, it just triggers it
on a schedule and surfaces failures to Airflow instead of hiding them.
"""
from __future__ import annotations

from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from openlineage_events import emit_openlineage_event

ML_ISOLATION_FOREST_URL = "http://ml-isolation-forest:8000"


def trigger_retrain(**_context) -> None:
    response = requests.post(f"{ML_ISOLATION_FOREST_URL}/retrain", timeout=120)
    response.raise_for_status()
    result = response.json()
    print(f"[isolation_forest_retrain] {result}")
    if result.get("status") == "skipped":
        print(f"[isolation_forest_retrain] skipped: {result.get('reason')} -- will retry on next schedule")


with DAG(
    dag_id="isolation_forest_retrain",
    description="Periodic retrain of the streaming anomaly-detection model",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datashield", "ml", "isolation-forest"],
) as dag:
    retrain = PythonOperator(
        task_id="retrain_isolation_forest",
        python_callable=trigger_retrain,
    )
    publish_lineage = PythonOperator(
        task_id="publish_openlineage_event",
        python_callable=emit_openlineage_event,
        op_kwargs={
            "inputs": [{"namespace": "datashield", "name": "validated_orders"}],
            "outputs": [{"namespace": "datashield", "name": "isolation_forest_model"}],
        },
    )
    retrain >> publish_lineage
