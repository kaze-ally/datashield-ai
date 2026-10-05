"""
services/schema_healer/healer/validator.py

Deliberately the *same* lightweight check used twice: once (optionally) to
characterize the incoming mismatch for the LLM prompt, and once — always,
non-optionally — after the mapping is applied. A healed record is only ever
published to `validated-data` if it passes this exact function; the LLM's
own self-reported `confidence` field is never trusted on its own (mirrors
rca_copilot's outcome_policy.py, which never lets a single signal decide
alone).

FIX (2026-09-23, from full-stack validation): `strict_extra_fields`
defaults to False now. An unrecognized field used to make the whole record
invalid, which meant every healing attempt on a record with so much as one
extra metadata field required the LLM to also emit a correct `drop_field`
op just to pass — on top of whatever rename/cast the actual mismatch
needed. New-but-unexpected fields are the single most common, most benign
form of schema drift (the schema-mapping literature treats "new optional
field appeared" as forward-compatible by default, not a breaking change),
so this raises the bar for a *successful heal* higher than the real-world
failure mode warrants. Unexpected fields are still recorded in
`ValidationResult.unexpected_fields` and surfaced to the LLM in the
diagnostic text either way — this only changes whether their mere presence
flips `valid` to False. Set `strict_extra_fields=True` (or
`healing.strict_extra_fields: true` in config) to restore the old,
stricter behavior if your contracts must reject any unrecognized field.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .contract_loader import DataContract

_TYPE_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
}


@dataclass
class ValidationResult:
    valid: bool
    missing_required: list[str] = field(default_factory=list)
    unexpected_fields: list[str] = field(default_factory=list)
    type_mismatches: list[str] = field(default_factory=list)
    invalid_enum_values: list[str] = field(default_factory=list)

    def as_diagnostic_text(self) -> str:
        parts = []
        if self.missing_required:
            parts.append(f"missing required fields: {sorted(self.missing_required)}")
        if self.unexpected_fields:
            parts.append(f"unexpected fields not in contract: {sorted(self.unexpected_fields)}")
        if self.type_mismatches:
            parts.append(f"type mismatches: {sorted(self.type_mismatches)}")
        if self.invalid_enum_values:
            parts.append(f"invalid enum values: {sorted(self.invalid_enum_values)}")
        return "; ".join(parts) if parts else "valid"


def validate_record(
    record: dict, contract: DataContract, strict_extra_fields: bool = False
) -> ValidationResult:
    result = ValidationResult(valid=True)
    contract_fields = {f.name: f for f in contract.fields}

    for name in contract.required_field_names():
        if name not in record or record[name] is None:
            result.missing_required.append(name)

    for name in record.keys():
        if name not in contract_fields:
            result.unexpected_fields.append(name)

    for name, value in record.items():
        f = contract_fields.get(name)
        if f is None or value is None:
            continue
        check = _TYPE_CHECKS.get(f.type)
        if check is not None and not check(value):
            result.type_mismatches.append(f"{name} (expected {f.type})")
        if f.allowed_values is not None and value not in f.allowed_values:
            result.invalid_enum_values.append(f"{name}={value!r}")

    blocking_unexpected = result.unexpected_fields if strict_extra_fields else []
    result.valid = not (
        result.missing_required
        or blocking_unexpected
        or result.type_mismatches
        or result.invalid_enum_values
    )
    return result
