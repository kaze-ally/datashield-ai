# Deploy DataShield AI on Render

The root `render.yaml` deploys the API services as private services, a public
dashboard and event bridge, Ollama with persistent model storage, and Render
Postgres. Kafka remains an external managed service.

## Before creating the Blueprint

1. Push this repository to GitHub. Render must be able to read the repository
   containing `render.yaml`.
2. Create a managed Kafka cluster that supports SASL over TLS. Keep its broker
   address, username, and password available; Render will ask for them while
   creating the Blueprint.
3. Check the plans in `render.yaml` before approving resource creation. The
   full stack uses multiple always-on services and the Ollama model host; it
   is not a free-tier deployment.

## Create the services

1. In Render, choose **New → Blueprint** and select the DataShield AI repo.
2. Review the resources from `render.yaml`. Supply the Kafka bootstrap address
   and SASL credentials when prompted. Use the same cluster and credentials
   for each service prompt.
3. Create the Blueprint and wait for Postgres and the private services to
   finish deploying. The dashboard service's pre-deploy command enables
   `pgvector` and applies the idempotent SQL files from `postgres/init/`.
4. Open the public `dashboard` service URL. The dashboard is served at `/`;
   `/health`, `/services`, `/events/recent`, and `/ws` are served by its bridge.

The dashboard bridge is the only public API entry point. Kafka and Ollama are
reachable from the backend services over their configured networks; the APIs
are private services.

The bridge's live event and WebSocket endpoints do not have authentication.
Anyone who can reach the public dashboard URL can read events currently
buffered by the bridge. Use demo data only until access control is added if
Kafka topics can contain customer or other sensitive records.

## Notes

- The bridge consumes Kafka continuously. If it cannot connect, confirm the
  bootstrap address and SASL settings in Render's service environment tabs.
- Model downloads run on the Ollama service's persistent disk. First startup
  can take several minutes, and inference is CPU-only on Render.
- The pipeline-observatory page still includes an Airflow status card. Airflow
  is not part of this Blueprint; scheduled drift checks should be invoked
  separately until a Render cron workflow is configured.
- Keep `.env` and Kafka credentials out of Git. Render stores the prompted
  values as environment variables.
