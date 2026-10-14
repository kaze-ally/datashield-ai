# Deploy DataShield AI on Render

The default `render.yaml` deploys a free static demo of the Pipeline Observatory.
The full backend stack is in `render-paid.yaml` and requires paid Render
services plus an external managed Kafka cluster.

## Before creating the Blueprint

1. Push this repository to GitHub. Render must be able to read it.
2. For the free static demo, no database, Kafka broker, or API credentials are
   needed. The page presents interactive architecture cards and sample traces;
   live services are disabled.
3. For the full stack, create a managed Kafka cluster that supports SASL over
   TLS and review the service plans in `render-paid.yaml`. It is not free-tier.

## Create the services

1. In Render, choose **New → Blueprint** and select the DataShield AI repo.
2. Keep the Blueprint path as `render.yaml` for the free demo. Select
   `render-paid.yaml` only if you want to provision the paid backend stack.
3. Deploy the Blueprint and open the static site's `onrender.com` URL.

## Notes

- Render static sites are free. Free web services sleep after 15 minutes idle;
  free Postgres expires after 30 days. The free demo avoids both limits by
  using neither a web server nor a database.
- Render's free plan doesn't include private services, so it can't host the
  microservice topology or Ollama model host at no cost.
- The paid dashboard bridge exposes buffered Kafka events publicly without
  authentication. Use demo data until access control is added if Kafka topics
  can contain customer or other sensitive records.
- Keep `.env` and Kafka credentials out of Git. Render stores prompted secret
  values as environment variables for the paid Blueprint.
