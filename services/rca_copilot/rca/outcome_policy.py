"""
rca/outcome_policy.py

Decides whether an RCA result is allowed to report "success" back to the
circuit breaker (letting it reset HALF_OPEN -> CLOSED) or must report
"failure" (keeping it OPEN, escalated to a human). Deliberately a small,
pure, exhaustively-testable function — this is the one place an LLM's
output is allowed to influence control flow, so the decision space is
bounded to a fixed enum the model was schema-constrained into, not an
open-ended interpretation of free text.

Two fail-safe defaults, both intentional:
  - Low-confidence severity classifications are treated as "high" — an
    uncertain model should not get to unlock automation resuming.
  - Only severities explicitly whitelisted in config can auto-reset the
    breaker; anything else (including a future severity value not yet
    accounted for in config) defaults to requiring a human.
"""
from __future__ import annotations

from dataclasses import dataclass

from rca.config import OutcomePolicyConfig
from rca.llm_client import RCAOutput


@dataclass
class OutcomeDecision:
    success: bool
    reason: str


def decide_outcome(rca: RCAOutput, policy: OutcomePolicyConfig) -> OutcomeDecision:
    if rca.confidence < policy.min_confidence_to_trust_severity:
        return OutcomeDecision(
            success=False,
            reason=(
                f"Confidence {rca.confidence:.2f} below trust threshold "
                f"{policy.min_confidence_to_trust_severity} — treating as high severity regardless of "
                f"reported severity='{rca.severity}'"
            ),
        )

    if rca.severity in policy.auto_reset_severities:
        return OutcomeDecision(
            success=True,
            reason=f"severity='{rca.severity}' is in auto_reset_severities, confidence={rca.confidence:.2f}",
        )

    return OutcomeDecision(
        success=False,
        reason=f"severity='{rca.severity}' is not in auto_reset_severities {policy.auto_reset_severities} — requires human review",
    )
