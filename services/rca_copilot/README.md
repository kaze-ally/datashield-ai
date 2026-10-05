# rca_copilot (Week 4)
Agentic root-cause-analysis copilot (Paper 6 / Microsoft "RCACopilot").
On anomaly/drift/circuit-breaker events, fetches Airflow DAG logs + Kafka
offsets, and calls the shared `llm_gateway` to synthesize a root-cause
narrative for the on-call engineer.
