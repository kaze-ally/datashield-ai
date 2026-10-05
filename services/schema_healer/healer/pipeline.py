"""
services/schema_healer/healer/pipeline.py

The single function that decides what happens to one mismatched record.
Takes an already-constructed LLM client and contract as arguments rather
than reaching for global state, so tests can pass a fake client and assert
on the outcome without Ollama, Kafka, or Postgres running — the same
dependency-injection shape `rca_copilot`'s pipeline uses to make
`test_integration_full_stack.py`'s "mock only the LLM call" possible.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from .circuit_guard import (
    CircuitGuardConfig,
    CircuitGuardState,
    check,
    record_failure,
    record_success,
)
from .contract_loader import DataContract
from .mapping_ops import MappingRejected, apply_mapping, parse_ops
from .validator import validate_record


class HealingLLMClient(Protocol):
    def propose_mapping(
        self, contract_yaml_text: str, record: dict, diagnostic_text: str
    ) -> object: ...  # returns something with .ops / .confidence / .narrative / .root_cause_category / .model_used


@dataclass
class HealingOutcome:
    success: bool
    healed_record: dict | None
    target_topic: str  # "validated-data" or "dead-letter-queue"
    reason: str
    root_cause_category: str
    narrative: str
    confidence: float
    model_used: str | None
    ops_applied: list[dict]
    new_guard_state: CircuitGuardState


MIN_CONFIDENCE_DEFAULT = 0.6


def _normalize_alias_name(name: str) -> tuple[str, ...]:
    tokens = re.findall(r"[a-z0-9]+", name.casefold())
    return tuple("total" if token in {"amt", "amount"} else token for token in tokens)


def _deterministic_alias_ops(
    record: dict,
    contract: DataContract,
    strict_extra_fields: bool,
) -> list[dict] | None:
    validation = validate_record(record, contract, strict_extra_fields=strict_extra_fields)
    if (
        len(validation.missing_required) != 1
        or validation.type_mismatches
        or validation.invalid_enum_values
        or (strict_extra_fields and len(validation.unexpected_fields) != 1)
    ):
        return None

    target = validation.missing_required[0]
    matching_sources = [
        source
        for source in validation.unexpected_fields
        if _normalize_alias_name(source) == _normalize_alias_name(target)
    ]
    if len(matching_sources) != 1:
        return None

    source = matching_sources[0]
    if record[source] is None:
        return None

    target_field = next(field for field in contract.fields if field.name == target)
    target_type = "integer" if target_field.type == "int" else target_field.type
    if target_type not in {"string", "integer", "float", "boolean"}:
        return None

    ops = [{"op": "rename_field", "source": source, "target": target}]
    if target_type != "string":
        ops.append({"op": "cast_type", "field": target, "target_type": target_type})
    return ops


def heal_one_record(
    *,
    record: dict,
    contract: DataContract,
    contract_yaml_text: str,
    diagnostic_text: str,
    llm_client: HealingLLMClient,
    guard_state: CircuitGuardState,
    guard_config: CircuitGuardConfig,
    min_confidence: float = MIN_CONFIDENCE_DEFAULT,
    strict_extra_fields: bool = False,
    now: datetime | None = None,
) -> HealingOutcome:
    from .circuit_guard import utcnow

    now = now or utcnow()

    decision = check(guard_state, now)
    if not decision.allowed:
        return HealingOutcome(
            success=False,
            healed_record=None,
            target_topic="dead-letter-queue",
            reason=decision.reason,
            root_cause_category="unknown",
            narrative="Skipped LLM call: contract is in cooldown after repeated failures.",
            confidence=0.0,
            model_used=None,
            ops_applied=[],
            new_guard_state=guard_state,  # unchanged — a paused skip doesn't count as a new failure
        )

    deterministic_ops = _deterministic_alias_ops(record, contract, strict_extra_fields)
    used_deterministic_fallback = deterministic_ops is not None
    if used_deterministic_fallback:
        proposal_ops = deterministic_ops
        proposal_confidence = 1.0
        proposal_root_cause = "field_renamed"
        proposal_narrative = "A unique contract-safe field alias was renamed and revalidated."
        proposal_model = None
    else:
        proposal = llm_client.propose_mapping(contract_yaml_text, record, diagnostic_text)
        proposal_ops = proposal.ops
        proposal_confidence = proposal.confidence
        proposal_root_cause = proposal.root_cause_category
        proposal_narrative = proposal.narrative
        proposal_model = proposal.model_used

    try:
        ops = parse_ops(proposal_ops)
    except MappingRejected as e:
        new_state = record_failure(guard_state, now, guard_config)
        return HealingOutcome(
            success=False,
            healed_record=None,
            target_topic="dead-letter-queue",
            reason=f"mapping_rejected: {e}",
            root_cause_category=proposal_root_cause,
            narrative=proposal_narrative,
            confidence=proposal_confidence,
            model_used=proposal_model,
            ops_applied=[],
            new_guard_state=new_state,
        )

    if not ops or proposal_confidence < min_confidence:
        new_state = record_failure(guard_state, now, guard_config)
        return HealingOutcome(
            success=False,
            healed_record=None,
            target_topic="dead-letter-queue",
            reason=f"low_confidence_or_no_ops: confidence={proposal_confidence:.2f} "
            f"(min={min_confidence}), ops={len(ops)}",
            root_cause_category=proposal_root_cause,
            narrative=proposal_narrative,
            confidence=proposal_confidence,
            model_used=proposal_model,
            ops_applied=[],
            new_guard_state=new_state,
        )

    healed = apply_mapping(record, ops)
    post_check = validate_record(healed, contract, strict_extra_fields=strict_extra_fields)

    if not post_check.valid and not used_deterministic_fallback:
        deterministic_ops = _deterministic_alias_ops(record, contract, strict_extra_fields)
        if deterministic_ops is not None:
            deterministic_ops = parse_ops(deterministic_ops)
            deterministic_healed = apply_mapping(record, deterministic_ops)
            deterministic_check = validate_record(
                deterministic_healed, contract, strict_extra_fields=strict_extra_fields
            )
            if deterministic_check.valid:
                ops = deterministic_ops
                healed = deterministic_healed
                post_check = deterministic_check
                used_deterministic_fallback = True
                proposal_confidence = 1.0
                proposal_root_cause = "field_renamed"
                proposal_narrative = "A unique contract-safe field alias was renamed and revalidated."
                proposal_model = None

    if not post_check.valid:
        new_state = record_failure(guard_state, now, guard_config)
        return HealingOutcome(
            success=False,
            healed_record=None,
            target_topic="dead-letter-queue",
            reason=f"post_heal_revalidation_failed: {post_check.as_diagnostic_text()}",
            root_cause_category=proposal_root_cause,
            narrative=proposal_narrative,
            confidence=proposal_confidence,
            model_used=proposal_model,
            ops_applied=[op.model_dump() for op in ops],
            new_guard_state=new_state,
        )

    new_state = record_success(guard_state)
    return HealingOutcome(
        success=True,
        healed_record=healed,
        target_topic="validated-data",
        reason=(
            "healed_and_revalidated_with_deterministic_rename"
            if used_deterministic_fallback
            else "healed_and_revalidated"
        ),
        root_cause_category=proposal_root_cause,
        narrative=proposal_narrative,
        confidence=proposal_confidence,
        model_used=proposal_model,
        ops_applied=[op.model_dump() for op in ops],
        new_guard_state=new_state,
    )
