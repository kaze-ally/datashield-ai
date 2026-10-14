# Free deployment (Render free web service, demo mode)

Runs the Pipeline Observatory dashboard + a synthetic live event feed
(raw -> validated, anomalies, schema healing, DLQ, drift, circuit breaker).
No Kafka, Airflow, Ollama or Postgres needed. Cost: $0.

1. `git add -A && git commit -m "Add free demo mode" && git push`
2. render.com -> New -> Web Service -> connect `kaze-ally/datashield-ai`.
3. Language: Docker. Dockerfile Path: `frontend/Dockerfile`. Root/Context: repo root. Instance Type: Free.
4. Environment: `DATASHIELD_DEMO_MODE` = `1`. Health Check Path: `/health`.
5. Create. Open the `onrender.com` URL.

Free-tier notes: the service sleeps after ~15 min idle (first request takes ~30-60 s to wake).
The full stack (8 services, Ollama, Kafka, Airflow) needs paid plans -- see DEPLOY_RENDER.md.
