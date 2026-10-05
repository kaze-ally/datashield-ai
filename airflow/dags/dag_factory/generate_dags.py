"""
DataShield AI — Config-Driven Dynamic DAG Generation (Innovation #1)
=====================================================================
Instead of hand-writing a new Airflow DAG file for every data-contract or
per-source pipeline, engineers (or the Sidecar Data Contract onboarding flow)
drop a YAML file into `dag_factory/configs/`. This module turns every config
into a live DAG at parse time using the `dag-factory` library.

Why this pattern (see chat response for full research citations):
  - Astronomer's `dag-factory` and `gusty` projects are the community-standard
    way to keep DAG definitions declarative and decoupled from Airflow's
    Python API, which matters here because Paper 9 (Data Mesh contracts)
    already pushes us toward YAML-first pipeline definitions.
  - New data-producer onboarding becomes a pull request that adds one YAML
    file — no Python, no redeploy of the Airflow image.

The orders example config calls the real Sidecar Validator, Drift Monitor,
and AI Catalog HTTP APIs in sequence. Business logic stays in those services;
model retraining is not wired into this example DAG.
"""
from pathlib import Path

from dagfactory import load_yaml_dags
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from openlineage_events import emit_openlineage_event

CONFIG_DIR = Path(__file__).parent / "configs"

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=str(CONFIG_DIR),
)

# dag-factory generates the business tasks from YAML. Attach a real Kafka
# lineage task to its final task so successful runs enrich the AI Catalog.
for generated_dag in [value for value in globals().values() if isinstance(value, DAG)]:
    if generated_dag.dag_id != "orders_validation_pipeline":
        continue
    lineage_task = PythonOperator(
        task_id="publish_openlineage_event",
        python_callable=emit_openlineage_event,
        op_kwargs={
            "inputs": [{"namespace": "datashield", "name": "raw-data"}],
            "outputs": [{"namespace": "datashield", "name": "orders_v1"}],
        },
        dag=generated_dag,
    )
    leaf_task_ids = [
        task.task_id
        for task in generated_dag.tasks
        if not task.downstream_task_ids
    ]
    for leaf_task_id in leaf_task_ids:
        if leaf_task_id != lineage_task.task_id:
            generated_dag.get_task(leaf_task_id) >> lineage_task
