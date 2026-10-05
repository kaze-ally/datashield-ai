"""
services/schema_healer/healer/mapping_ops.py

The safety boundary of the whole service.

Paper 8's own description says the LLM "writes a temporary Python translation
script to map the new schema to the expected format." Taken literally, that
means exec()-ing LLM output against production payloads — the exact anti-pattern
flagged by the current sandboxing literature (AST/import/builtin allow-listing,
process isolation, resource limits — see README_week5_schema_healer.md's
"Research & Innovation Breakdown" for citations). It's also unnecessary: the
schema-mapping literature (e.g. arXiv:2505.24716) treats the LLM's job as
*producing an alignment/mapping*, not as a code generator.

So this service never asks the LLM for code. It asks for a small JSON list of
operations drawn from a fixed, closed vocabulary (see `Op`), validated with
pydantic, and applied here by ordinary Python functions. There is no eval(),
no exec(), no dynamic attribute access driven by LLM output beyond a dict
key lookup guarded by an allow-list. An LLM that returns anything outside the
vocabulary fails validation before a single field is touched.

Every op is a pure function: (record: dict, op) -> dict. `apply_mapping`
folds the whole ordered list over the record and returns a new dict; the
input is never mutated in place, so a failed/partial apply can't corrupt the
original message that still needs to go to the DLQ.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

# ---------------------------------------------------------------------------
# Closed operation vocabulary
#
# These models are the strict response-validation boundary. The simpler
# schema sent to Ollama lives in llm_client.py because Ollama's constrained
# decoder does not reliably accept this discriminated union's oneOf/$ref form.
# These models are the strict response-validation boundary. The simpler
# schema sent to Ollama lives in llm_client.py because Ollama's constrained
# decoder does not reliably accept this discriminated union's oneOf/$ref form.
# ---------------------------------------------------------------------------

_ALLOWED_TYPES = {"string", "integer", "float", "boolean"}

_STRICT = ConfigDict(extra="forbid")


class RenameField(BaseModel):
    model_config = _STRICT
    op: Literal["rename_field"]
    source: str
    target: str


class CastType(BaseModel):
    model_config = _STRICT
    op: Literal["cast_type"]
    field: str
    target_type: Literal["string", "integer", "float", "boolean"]


class SetDefault(BaseModel):
    model_config = _STRICT
    op: Literal["set_default"]
    field: str
    value: Any


class DropField(BaseModel):
    model_config = _STRICT
    op: Literal["drop_field"]
    field: str


class ConstMap(BaseModel):
    model_config = _STRICT
    op: Literal["const_map"]
    field: str
    mapping: dict[str, Any]
    # value used when the observed value isn't a key in `mapping`
    fallback: Any = None


MappingOp = Union[RenameField, CastType, SetDefault, DropField, ConstMap]

# Discriminated union: Pydantic uses this to validate each response op against
# its own required fields after Ollama's flat-schema constrained generation.
MappingOpUnion = Annotated[MappingOp, Field(discriminator="op")]

RootCauseCategory = Literal[
    "field_renamed",
    "new_optional_field",
    "new_required_field",
    "field_removed",
    "type_widened",
    "type_narrowed",
    "enum_value_added",
    "unknown",
]


class HealingLLMResponse(BaseModel):
    """The full structured-output contract for the healer's LLM call.
    `llm_client.py` validates the raw response against this model after
    Ollama returns JSON constrained by its simpler flat request schema."""

    ops: list[MappingOpUnion]
    root_cause_category: RootCauseCategory = "unknown"
    narrative: str = ""
    confidence: float = Field(ge=0.0, le=1.0)


_OP_MODELS: dict[str, type[BaseModel]] = {
    "rename_field": RenameField,
    "cast_type": CastType,
    "set_default": SetDefault,
    "drop_field": DropField,
    "const_map": ConstMap,
}

MAX_OPS_PER_MAPPING = 25  # a mapping this large is almost certainly a bad LLM output


class MappingRejected(Exception):
    """Raised when the LLM's proposed mapping fails allow-list validation."""


@dataclass
class OpResult:
    ok: bool
    detail: str


def parse_ops(raw_ops: list[dict]) -> list[MappingOp]:
    """Validate a raw list of dicts (as returned by the LLM's structured
    output) against the closed vocabulary. Raises MappingRejected on any
    unknown op name, missing field, or wrong type — never partially trusts
    a malformed entry."""
    if not isinstance(raw_ops, list):
        raise MappingRejected("mapping payload is not a list")
    if len(raw_ops) > MAX_OPS_PER_MAPPING:
        raise MappingRejected(
            f"mapping has {len(raw_ops)} ops, exceeds MAX_OPS_PER_MAPPING={MAX_OPS_PER_MAPPING}"
        )

    parsed: list[MappingOp] = []
    for i, raw in enumerate(raw_ops):
        if not isinstance(raw, dict) or "op" not in raw:
            raise MappingRejected(f"op[{i}] missing 'op' discriminator: {raw!r}")
        model_cls = _OP_MODELS.get(raw["op"])
        if model_cls is None:
            raise MappingRejected(f"op[{i}] unknown op name: {raw['op']!r}")
        try:
            parsed.append(model_cls.model_validate(raw))
        except ValidationError as e:
            raise MappingRejected(f"op[{i}] ({raw['op']}) failed validation: {e}") from e
    return parsed


def _cast_value(value: Any, target_type: str) -> Any:
    if target_type not in _ALLOWED_TYPES:
        raise MappingRejected(f"cast_type target_type not in allow-list: {target_type!r}")
    if value is None:
        return None
    if target_type == "string":
        return str(value)
    if target_type == "integer":
        return int(float(value))
    if target_type == "float":
        return float(value)
    if target_type == "boolean":
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y"}
        return bool(value)
    raise MappingRejected(f"unreachable cast target_type: {target_type!r}")  # pragma: no cover


def apply_op(record: dict, op: MappingOp) -> dict:
    """Apply a single validated op to a copy of `record`. Never mutates the
    input. Missing source fields are no-ops rather than raising, so one bad
    op in a list doesn't blow up the whole mapping — the post-heal contract
    re-validation step is what ultimately decides success/failure, not this
    layer."""
    out = dict(record)

    if isinstance(op, RenameField):
        if op.source in out:
            out[op.target] = out.pop(op.source)
        return out

    if isinstance(op, CastType):
        if op.field in out:
            out[op.field] = _cast_value(out[op.field], op.target_type)
        return out

    if isinstance(op, SetDefault):
        if op.field not in out or out[op.field] is None:
            out[op.field] = op.value
        return out

    if isinstance(op, DropField):
        out.pop(op.field, None)
        return out

    if isinstance(op, ConstMap):
        if op.field in out:
            key = str(out[op.field])
            out[op.field] = op.mapping.get(key, op.fallback)
        return out

    raise MappingRejected(f"unhandled op type: {type(op).__name__}")  # pragma: no cover


def apply_mapping(record: dict, ops: list[MappingOp]) -> dict:
    """Fold every op over a copy of the record, in order, and return the
    result. Pure — no I/O, no globals, no eval/exec."""
    healed = dict(record)
    for op in ops:
        healed = apply_op(healed, op)
    return healed
