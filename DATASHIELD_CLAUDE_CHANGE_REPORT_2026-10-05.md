# DataShield AI - Change and Troubleshooting Report

Date: 2026-10-05
Project: `datashield-ai`
Purpose: Handoff for Claude describing the fixes, current behavior, verification, and remaining operational details for the local Pipeline Observatory.

## Current status

The local stack is running. The Observatory is connected to the local bridge and reports nine reachable services. Kafka is connected, the validation sink and Isolation Forest consumer are alive, and the dashboard receives actual service payloads. The current drift circuit breaker is `OPEN` with its cooldown elapsed; this is persisted state from earlier drift incidents, not a UI failure. The next drift incident can exercise the half-open probe. The anomaly breaker is `CLOSED`.

Frontend: `frontend/DataShield AI — Pipeline Observatory.html`
Local startup: `run-local.ps1`

## Problems found and fixes made

| Area | Problem | Fix / result |
|---|---|---|
| Observatory data | Some cards showed illustrative or stale values as if they were live, and the page did not consistently explain empty results. | Updated the page to poll the local bridge, render service payloads, and distinguish live results from illustrative content or no activity. The dashboard covers service health, drift/schema reports, Isolation Forest, breaker states/events, RCA, and lineage. |
| Bridge/API coverage | The bridge lacked endpoints or summaries for several persisted reports and subsystem states. | Added bridge access to drift reports, schema guard/report, drift and anomaly breaker state/events, RCA reports for both monitors, Isolation Forest model information, and sidecar health. Added a read-only drift reports endpoint. |
| Isolation Forest features | Training and inference did not use matching feature inputs: training data often lacked currency and the timestamp field names differed. | Standardized the model to `order_total` and `hour_of_day`, accepted `created_at`, `order_timestamp`, and `ingested_at`, validated saved model feature names, and retrained incompatible models. Model information now reports actual scored and anomalous counts and consumer health. |
| Isolation Forest output visibility | The UI could imply that a trained model had already scored records. | The Observatory now separates model training from actual inference counts and shows the latest prediction. A representative normal order was scored non-anomalous; high-value test orders were scored anomalous. |
| Anomaly pipeline | Isolation Forest alerts were not consistently represented as incidents for breaker and RCA processing. | Added structured anomaly event/report identifiers and monitor metadata; configured the anomaly topic in the breaker; forwarded incident payloads through remediation requests and RCA context/prompt. The Observatory now reads anomaly breaker and RCA results. |
| Existing database schema | The running database volume had not applied schema-healing tables, so schema report/guard requests failed with missing-relation errors. | Applied `postgres/init/005_schema_healing.sql` to the existing database. Schema report and guard endpoints then worked. |
| Validated-order sink | PostgreSQL rejected `ON CONFLICT (order_id)` because the running database did not have the required unique constraint. Kafka sink retries also risked losing the consumer thread after a long retry. | Checked for duplicate IDs, applied `postgres/init/006_validated_order_stream_sink.sql` as the database owner, and made the sidecar sink reconnect/rejoin on failure. Health now reflects whether the sink consumer is alive. |
| Breaker state display | A drift breaker persisted as `OPEN` after old failures, and the UI could make its readiness ambiguous. | Added `cooldown_elapsed` to breaker state and displayed `OPEN / probe ready` when appropriate. The state is intentionally not reset; the next incident exercises the half-open path. |

## Isolation Forest verification

The model was rebuilt and trained with 2,000 samples using the two declared features: `order_total` and `hour_of_day`. Five local verification orders were processed: one typical order and four high-value orders. The model recorded 5 processed records and 4 anomalies. The typical order had a positive decision score (`0.00715`) and was not flagged; a high-value order had a negative score (`-0.15437`) and was flagged. This is consistent with scikit-learn Isolation Forest semantics: `decision_function` values below zero and `predict` value `-1` indicate an outlier. These examples validate the code path, not the model's performance on a representative production dataset.

The anomaly RCA path also produced a report for monitor `isolation_forest_anomaly`. It was high severity and marked for human review, so its outcome was not auto-approved.

## Files changed

- `frontend/DataShield AI — Pipeline Observatory.html` - live dashboard rendering and summaries.
- `frontend/datashield_bridge.py` - local bridge endpoints and service/report aggregation.
- `services/drift_monitor/main.py` - persisted drift reports read endpoint.
- `services/ml_isolation_forest/main.py` - feature consistency, model validation/metadata, anomaly envelope, and health.
- `services/circuit_breaker/config/circuit_breaker_config.yaml` - anomaly alert source configuration.
- `services/circuit_breaker/breaker/kafka_consumer.py` - carry incident details to remediation.
- `services/circuit_breaker/main.py` - report cooldown readiness in state responses.
- `services/rca_copilot/rca/context_loader.py` - incident context support.
- `services/rca_copilot/rca/kafka_consumer.py` - include incident details in RCA generation.
- `services/sidecar_validator/main.py` - resilient validated-order Kafka sink and accurate health status.
- `tests/test_ml_isolation_forest.py` - Isolation Forest behavior coverage.

Database migrations applied to the existing local volume: `postgres/init/005_schema_healing.sql` and `postgres/init/006_validated_order_stream_sink.sql`.

## Verification performed

- `python -m pytest tests -q`: **108 passed**, 223 warnings, in 2.25 seconds. Warnings were dependency and FastAPI lifecycle deprecations; there were no test failures.
- Python compilation checks passed for the bridge and modified service modules.
- The inline JavaScript in the Observatory passed `node --check`.
- Live local checks confirmed nine reachable services, Kafka connected, sidecar sink alive, Isolation Forest consumer alive, schema guard working, drift reports available, and anomaly breaker/RCA data visible.

## Remaining state and notes for continuation

- The drift breaker is persisted `OPEN` and cooldown-ready. Do not describe it as closed or clear it without a deliberate state-reset decision.
- The five verification records were removed from `validated_orders` after checking persistence to avoid skewing drift results. Their feature/inference evidence and related Kafka anomaly messages remain; an anomaly RCA report remains available.
- The Isolation Forest check used a small, deliberately varied local sample. It validates integration and expected scoring direction, not a production accuracy claim.
- Some services emit framework/dependency deprecation warnings. They did not affect the passing test suite.

Suggested Claude prompt: “Review this handoff and the current repository state. Continue from the described local state, verify any assumptions you need, and avoid replaying the five verification orders or resetting the persisted drift breaker unless required.”
