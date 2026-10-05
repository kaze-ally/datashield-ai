"""
tests/test_llm_client.py — no real Ollama needed; httpx.post is mocked.

Exercises the flat Ollama request schema, strict response parsing, model
fallbacks, and the live-test failure shapes without requiring Ollama.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from services.schema_healer.healer.llm_client import (
    FAIL_SAFE_PLACEHOLDER,
    OllamaHealerClient,
)


def _fake_ollama_response(message_content: dict | str):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    content = message_content if isinstance(message_content, str) else json.dumps(message_content)
    resp.json.return_value = {"message": {"content": content}}
    return resp


def _client():
    return OllamaHealerClient(
        base_url="http://ollama:11434", model="phi4-mini", fallback_model="llama3.1:8b"
    )


COMPLETE_VALID_BODY = {
    "ops": [
        {"op": "rename_field", "source": "order_amt", "target": "order_total"},
        {"op": "cast_type", "field": "order_total", "target_type": "float"},
    ],
    "root_cause_category": "field_renamed",
    "narrative": "Upstream renamed the field.",
    "confidence": 0.9,
}

# This is the exact malformed body the live test observed from phi4-mini —
# the discriminator is present but the op-specific required fields are not.
BARE_OP_ONLY_BODY = {
    "ops": [{"op": "rename_field"}],
    "root_cause_category": "field_renamed",
    "narrative": "renamed",
    "confidence": 0.9,
}


def test_complete_valid_response_becomes_a_proposal():
    with patch("httpx.post", return_value=_fake_ollama_response(COMPLETE_VALID_BODY)) as mock_post:
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert mock_post.call_count == 1  # primary model succeeded, no fallback needed
    assert proposal.model_used == "phi4-mini"
    assert proposal.confidence == 0.9
    assert len(proposal.ops) == 2
    assert proposal.ops[0] == {"op": "rename_field", "source": "order_amt", "target": "order_total"}


def test_bare_op_reproduces_live_test_failure_and_triggers_fallback():
    # Primary model returns the incomplete op observed live; fallback model
    # (second call) returns a complete, valid one.
    with patch(
        "httpx.post",
        side_effect=[
            _fake_ollama_response(BARE_OP_ONLY_BODY),
            _fake_ollama_response(COMPLETE_VALID_BODY),
        ],
    ) as mock_post:
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert mock_post.call_count == 2  # confirms the fallback retry actually fired
    assert proposal.model_used == "llama3.1:8b"
    assert len(proposal.ops) == 2


def test_bare_op_on_both_models_returns_fail_safe_placeholder():
    with patch(
        "httpx.post",
        side_effect=[
            _fake_ollama_response(BARE_OP_ONLY_BODY),
            _fake_ollama_response(BARE_OP_ONLY_BODY),
        ],
    ) as mock_post:
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert mock_post.call_count == 2
    assert proposal.model_used is None
    assert proposal.ops == FAIL_SAFE_PLACEHOLDER["ops"]
    assert proposal.confidence == 0.0


def test_missing_required_top_level_field_is_handled_safely():
    # confidence is required on HealingLLMResponse; omit it entirely.
    incomplete = {"ops": [], "root_cause_category": "unknown", "narrative": ""}
    with patch(
        "httpx.post",
        side_effect=[_fake_ollama_response(incomplete), _fake_ollama_response(incomplete)],
    ):
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert proposal.model_used is None
    assert proposal.confidence == 0.0


def test_non_json_content_is_handled_safely():
    with patch(
        "httpx.post",
        side_effect=[
            _fake_ollama_response("not json at all"),
            _fake_ollama_response("still not json"),
        ],
    ):
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert proposal.model_used is None


def test_http_error_falls_back_then_fails_safe():
    import httpx

    def raise_http_error(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    with patch("httpx.post", side_effect=raise_http_error) as mock_post:
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert mock_post.call_count == 2  # primary attempt + fallback attempt
    assert proposal.model_used is None
    assert proposal.confidence == 0.0


def test_ollama_invalid_schema_500_triggers_fallback():
    import httpx

    response = httpx.Response(
        500,
        json={"error": "invalid JSON schema in format"},
        request=httpx.Request("POST", "http://ollama:11434/api/chat"),
    )
    with patch(
        "httpx.post",
        side_effect=[response, _fake_ollama_response(COMPLETE_VALID_BODY)],
    ) as mock_post:
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")

    assert mock_post.call_count == 2
    assert mock_post.call_args_list[0].kwargs["json"]["format"]["type"] == "object"
    assert proposal.model_used == "llama3.1:8b"
    assert proposal.ops == COMPLETE_VALID_BODY["ops"]


def test_no_fallback_configured_only_calls_once():
    client = OllamaHealerClient(base_url="http://ollama:11434", model="phi4-mini", fallback_model="")
    with patch("httpx.post", return_value=_fake_ollama_response(BARE_OP_ONLY_BODY)) as mock_post:
        proposal = client.propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert mock_post.call_count == 1
    assert proposal.model_used is None


def test_empty_ops_with_valid_confidence_is_a_legitimate_proposal():
    # The model honestly saying "I can't fix this" (empty ops, low
    # confidence) must NOT be treated as a validation failure — it's a
    # valid HealingLLMResponse, just an unhelpful one. The pipeline (not
    # this client) decides what to do with low confidence / no ops.
    body = {"ops": [], "root_cause_category": "unknown", "narrative": "not sure", "confidence": 0.1}
    with patch("httpx.post", return_value=_fake_ollama_response(body)) as mock_post:
        proposal = _client().propose_mapping("contract_name: orders_v1", {}, "diagnostic")
    assert mock_post.call_count == 1  # no fallback triggered — this was a valid response
    assert proposal.model_used == "phi4-mini"
    assert proposal.ops == []
    assert proposal.confidence == 0.1
