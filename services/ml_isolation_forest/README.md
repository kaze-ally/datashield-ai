# ml_isolation_forest (Week 2)
Streaming scikit-learn IsolationForest inference over `validated-data`.
Implements Papers 1 & 3. Publishes flagged records to `anomaly-alerts`.
Retrained periodically by the `isolation_forest_retrain` Airflow DAG.
