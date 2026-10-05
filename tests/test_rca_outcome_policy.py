"""
tests/test_rca_outcome_policy.py

Pure logic tests for rca/outcome_policy.py — the one place an LLM output
is allowed to influence control flow. No Postgres, no Kafka, no LLM needed.
"""
import pytest

from rca.config import OutcomePolicyConfig
from rca.llm_client import RCAOutput
from rca.outcome_policy import decide_outcome


@pytest.fixture
def policy() -> OutcomePolicyConfig:
    return OutcomePolicyConfig(auto_reset_severities=["low", "medium"], min_confidence_to_trust_severity=0.5)


def _rca(severity: str, confidence: float, category: str = "legitimate_business_shift") -> RCAOutput:
    return RCAOutput(root_cause_category=category, narrative="test", severity=severity, confidence=confidence, model_used="test-model")


def test_low_severity_high_confidence_succeeds(policy):
    decision = decide_outcome(_rca("low", 0.9), policy)
    assert decision.success is True


def test_medium_severity_high_confidence_succeeds(policy):
    decision = decide_outcome(_rca("medium", 0.8), policy)
    assert decision.success is True


def test_high_severity_always_fails_regardless_of_confidence(policy):
    decision = decide_outcome(_rca("high", 0.99), policy)
    assert decision.success is False


def test_low_confidence_overrides_low_severity_to_failure(policy):
    """The core fail-safe: an uncertain model must not get to unlock automation resuming."""
    decision = decide_outcome(_rca("low", 0.1), policy)
    assert decision.success is False
    assert "confidence" in decision.reason.lower()


def test_confidence_exactly_at_threshold_is_trusted(policy):
    decision = decide_outcome(_rca("low", 0.5), policy)
    assert decision.success is True


def test_unknown_severity_value_defaults_to_failure(policy):
    """A severity value not in the whitelist (e.g. a future enum addition) must fail safe, not succeed by default."""
    decision = decide_outcome(_rca("catastrophic", 0.9), policy)
    assert decision.success is False


def test_llm_unavailable_placeholder_always_fails(policy):
    """The fail-safe placeholder from llm_client.py (severity='high', confidence=0.0) must never succeed."""
    decision = decide_outcome(_rca("high", 0.0), policy)
    assert decision.success is False


def test_stricter_policy_configuration_is_respected(policy):
    strict_policy = OutcomePolicyConfig(auto_reset_severities=["low"], min_confidence_to_trust_severity=0.9)
    assert decide_outcome(_rca("medium", 0.95), strict_policy).success is False
    assert decide_outcome(_rca("low", 0.95), strict_policy).success is True
    assert decide_outcome(_rca("low", 0.5), strict_policy).success is False  # below the stricter 0.9 threshold
