#!/usr/bin/env bash
# =============================================================================
# DataShield AI — Kafka topic bootstrap
# Runs once via the `kafka-init` service, then exits.
# =============================================================================
set -euo pipefail

BOOTSTRAP="kafka:9092"

TOPICS=(
  "raw-data:6:1"                     # P1/P2 producers -> validation sidecar
  "validated-data:6:1"               # post data-contract validation
  "dead-letter-queue:3:1"            # failed healing / max circuit-breaker attempts
  "remediation-requests:1:1"          # circuit-breaker incidents -> RCA Copilot
  "remediation-dlq:1:1"               # failed remediation requests
  "anomaly-alerts:3:1"                # Isolation Forest output
  "drift-events:3:1"                  # Evidently AI output
  "schema-evolution-requests:3:1"    # LLM schema healer input
  "schema-events:3:1"                 # NEW: lightweight schema-fingerprint events -> AI catalog
  "openlineage-events:3:1"           # NEW: lineage events -> AI catalog
  "catalog-updates:3:1"              # NEW: AI-enriched catalog change feed
)

echo "[kafka-init] waiting for broker at ${BOOTSTRAP}..."
cub kafka-ready -b "${BOOTSTRAP}" 1 30

for entry in "${TOPICS[@]}"; do
  IFS=":" read -r name partitions replication <<< "$entry"
  echo "[kafka-init] ensuring topic '${name}' (partitions=${partitions}, rf=${replication})"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor "${replication}"
done

echo "[kafka-init] topic bootstrap complete."
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
