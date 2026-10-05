# schema_healer (Week 3)
LLM-assisted schema evolution (Paper 8). On a data-contract mismatch,
calls the shared `llm_gateway` to propose a translation mapping from the
new payload shape to the expected contract shape. Bounded by
`circuit_breaker` so failed healing can't loop indefinitely.

The healer consumes validated mismatch envelopes from
`schema-evolution-requests`, then publishes successful revalidations to
`validated-data` and failed attempts to `dead-letter-queue`.
