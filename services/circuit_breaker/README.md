# circuit_breaker (Week 4)
Remediation circuit breaker (Paper 7). Tracks consecutive auto-heal
attempts per data contract; after `max_heal_attempts` (see contract YAML)
it halts automated action, routes to the dead-letter queue, and pages
`rca_copilot` for a human-readable alert instead of looping forever.
