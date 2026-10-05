# DataShield AI

Automated, real-time data observability, privacy enforcement, and
autonomous data-healing platform for e-commerce pipelines. Full literature
review and paper-by-paper justification: [`docs/datashield_ai_comprehensive_architecture.md`](docs/datashield_ai_comprehensive_architecture.md).

## Quickstart

From PowerShell, start the complete local stack and the host Kafka bridge with:

```powershell
.\run-local.ps1
```

Use `.un-local.ps1 -Build` after changing service code or Dockerfiles. Open
`frontend\DataShield AI — Pipeline Observatory.html` locally; it connects to
the bridge automatically. The bridge serves live service status and Kafka
events at `http://localhost:8099`.

Nothing to sign up for, no API key required — `ollama-init` pulls two small
open models on first start, which takes a few minutes and a couple GB of
disk. Everything after that runs entirely on your machine.

**Have an NVIDIA GPU?** `docker-compose.yml` already requests GPU passthrough
for the `ollama` service. You need the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
installed on the host (Windows: enable GPU support in Docker Desktop's WSL2
backend). Check your VRAM first — `nvidia-smi --query-gpu=name,memory.total --format=csv`
— and set `LLM_CHEAP_MODEL`/`LLM_STRONG_MODEL` in `.env` accordingly:

| VRAM | Cheap tier | Strong tier |
|---|---|---|
| 4GB | `phi4-mini` | `llama3.1:8b` (partial CPU spillover) |
| 6GB | `llama3.2:3b` | `qwen3:8b` |
| 8GB | `llama3.2:3b` | `llama3.1:8b` (fully GPU-resident) |

No GPU, or don't want to bother with the toolkit? Delete the `deploy:` block
under the `ollama` service — it'll fall back to CPU automatically, just slower.

| Service            | URL                              | Notes                          |
|---------------------|-----------------------------------|---------------------------------|
| Airflow             | http://localhost:18080            | admin / admin (from `.env`)     |
| Kafka UI             | http://localhost:8081             | topic browser                   |
| Dashboard            | http://localhost:8501             | Streamlit health/catalog view   |
| LLM Gateway          | http://localhost:8090/health       | semantic cache + model cascade  |
| AI Catalog           | http://localhost:8091/catalog      | AI-enriched dataset descriptions|
| Sidecar Validator    | http://localhost:8092/contracts    | loaded data contracts           |
| Ollama               | http://localhost:11434/api/tags    | local model server (dev only)   |
| Postgres             | localhost:5432                     | `airflow` + `datashield` DBs    |

## Architecture

Nine research papers (see docs/) justify the core pipeline:
Kafka → sidecar contract validation → Isolation Forest anomaly detection +
Evidently AI drift monitoring → circuit-breaker-gated auto-healing → agentic
RCA copilot, orchestrated by Airflow.

### New for this build (Chief Innovator review)

1. **Config-Driven Dynamic DAG Generation** — `airflow/dags/dag_factory/`.
   New data producers are onboarded via a YAML file, not a new Python DAG.
2. **LLM Cost-Optimization Gateway** — `services/llm_gateway/`. FrugalGPT-style
   semantic cache + small→larger model cascade, shared by every LLM-calling
   agent (RCA Copilot, Schema Healer, AI Catalog) — and now backed entirely
   by self-hosted Ollama models instead of a paid API, so the whole stack
   runs for $0 in inference cost.
3. **AI-Enriched Data Catalog** — `services/ai_catalog/`. Lineage/schema
   events get LLM-generated descriptions and tags automatically, instead of
   waiting on manual documentation. Airflow publishes OpenLineage-compatible
   completion events to Kafka, and the catalog persists dataset edges.

## Roadmap (6-week MVP)

- **Week 1 (this drop):** Infra skeleton — Airflow, Kafka/Zookeeper, Postgres
  (pgvector), dashboard/LLM-gateway/AI-catalog scaffolds, dynamic DAG factory.
- **Week 2:** Sidecar data-contract validator + Isolation Forest streaming
  inference + OpenLineage emission from Airflow.
- **Week 3:** Evidently AI drift monitor + LLM schema healer.
- **Week 4:** Remediation circuit breaker + RCA Copilot agent.
- **Week 5:** Dashboard goes live end-to-end (anomaly/drift charts, RCA feed,
  catalog search); cost-optimization metrics surfaced (cache hit rate,
  cascade escalation rate).
- **Week 6:** Hardening, load testing, demo polish.

## Repository layout

See the directory tree in the project chat for the full annotated layout;
each `services/*/README.md` states which week that service is built out.
