"""
rca/llm_client.py

Calls Ollama's /api/chat with a JSON-schema `format` to force structured
output (narrative + root_cause_category + severity + confidence) rather
than parsing free-text — per Ollama's own docs, schema-constrained
decoding is materially more reliable than asking nicely for JSON in the
prompt. Falls back to a second model once on failure/timeout before giving
up, since a single flaky inference call shouldn't silently drop an
incident's diagnosis.

NOTE: calls Ollama directly. DataShield's llm_gateway service already
implements semantic caching + a cheap-to-strong model cascade — swapping
this to call the gateway instead (once its exact request/response contract
is confirmed) would let RCA reuse that instead of duplicating it. Flagged
in README_week4_part2.md as a follow-up, not done here since guessing the
gateway's contract wrong would be a correctness risk worse than the
duplication.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "root_cause_category": {
            "type": "string",
            "enum": [
                "legitimate_business_shift", "upstream_data_bug",
                "threshold_miscalibration", "seasonal_pattern", "unknown",
            ],
        },
        "narrative": {"type": "string"},
        "severity": {"type": "string", "enum": ["low", "medium", "high"]},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    },
    "required": ["root_cause_category", "narrative", "severity", "confidence"],
}


@dataclass
class RCAOutput:
    root_cause_category: str
    narrative: str
    severity: str
    confidence: float
    model_used: str


def _build_prompt(context_summary: str) -> str:
    return (
        "You are a root-cause analysis assistant for a data pipeline observability "
        "system. Given the diagnostic evidence below, classify the likely root cause "
        "category, write a short (2-4 sentence) plain-language narrative an on-call "
        "engineer could read in a Slack alert, and rate severity honestly — 'high' "
        "means this needs a human before any automation resumes, 'low'/'medium' mean "
        "it's understood well enough to be safe to auto-resume monitoring.\n\n"
        f"Evidence:\n{context_summary}"
    )


def _call_model(base_url: str, model: str, prompt: str, timeout: float) -> Dict[str, Any]:
    resp = requests.post(
        f"{base_url}/api/chat",
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "format": RESPONSE_SCHEMA,
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    content = resp.json()["message"]["content"]
    return json.loads(content)  # schema-constrained, but still validate — see the serverman.co.uk finding in README


def generate_rca(
    base_url: str,
    model: str,
    fallback_model: str,
    timeout_seconds: float,
    context_summary: str,
) -> RCAOutput:
    prompt = _build_prompt(context_summary)

    for attempt_model in (model, fallback_model):
        try:
            parsed = _call_model(base_url, attempt_model, prompt, timeout_seconds)
            return RCAOutput(
                root_cause_category=parsed["root_cause_category"],
                narrative=parsed["narrative"],
                severity=parsed["severity"],
                confidence=float(parsed["confidence"]),
                model_used=attempt_model,
            )
        except Exception:
            logger.exception("RCA generation failed with model=%s, trying fallback if available", attempt_model)

    # Both attempts failed — fail safe, not silent. A missing narrative
    # must not look like a clean "low severity, all good" result; treat
    # it as "high" so the breaker stays cautious and a human gets involved.
    logger.error("Both primary and fallback LLM calls failed — returning a fail-safe high-severity placeholder")
    return RCAOutput(
        root_cause_category="unknown",
        narrative="RCA generation failed (LLM unavailable or errored on both primary and fallback models). "
                  "Manual investigation required — see drift_reports for the underlying evidence.",
        severity="high",
        confidence=0.0,
        model_used="none",
    )
