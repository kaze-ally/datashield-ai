# sidecar_validator (Week 2)
Enforces the YAML data contracts (see /contracts) against messages on
`raw-data` before they reach `validated-data`. Implements Paper 9's
declarative sidecar pattern. On violation, routes to `schema_healer`
(healing attempts governed by `circuit_breaker`) or the DLQ.

Contract mismatches are sent as `schema_mismatch` events to
`schema-evolution-requests`; the separate `schema-events` topic is reserved
for schema fingerprints consumed by the AI catalog.
