"""tests/test_mapping_ops.py — no infra needed."""
import pytest

from services.schema_healer.healer.mapping_ops import (
    MAX_OPS_PER_MAPPING,
    MappingRejected,
    apply_mapping,
    apply_op,
    parse_ops,
)


def test_parse_ops_accepts_valid_vocabulary():
    raw = [
        {"op": "rename_field", "source": "order_amt", "target": "order_total"},
        {"op": "cast_type", "field": "order_total", "target_type": "float"},
        {"op": "set_default", "field": "currency", "value": "USD"},
        {"op": "drop_field", "field": "internal_debug_flag"},
        {"op": "const_map", "field": "pmt", "mapping": {"cc": "credit_card"}, "fallback": "unknown"},
    ]
    ops = parse_ops(raw)
    assert len(ops) == 5


def test_parse_ops_rejects_unknown_op_name():
    with pytest.raises(MappingRejected, match="unknown op name"):
        parse_ops([{"op": "exec_python", "code": "import os; os.system('rm -rf /')"}])


def test_parse_ops_rejects_missing_discriminator():
    with pytest.raises(MappingRejected, match="missing 'op'"):
        parse_ops([{"source": "a", "target": "b"}])


def test_parse_ops_rejects_malformed_op_payload():
    # cast_type requires target_type in a closed enum
    with pytest.raises(MappingRejected):
        parse_ops([{"op": "cast_type", "field": "x", "target_type": "python_object"}])


def test_parse_ops_rejects_oversized_mapping():
    raw = [{"op": "drop_field", "field": f"f{i}"} for i in range(MAX_OPS_PER_MAPPING + 1)]
    with pytest.raises(MappingRejected, match="exceeds MAX_OPS_PER_MAPPING"):
        parse_ops(raw)


def test_parse_ops_rejects_non_list_payload():
    with pytest.raises(MappingRejected, match="not a list"):
        parse_ops({"op": "drop_field", "field": "x"})  # dict, not list


def test_apply_rename_field():
    ops = parse_ops([{"op": "rename_field", "source": "order_amt", "target": "order_total"}])
    healed = apply_mapping({"order_amt": 12.5, "order_id": "o1"}, ops)
    assert healed == {"order_total": 12.5, "order_id": "o1"}


def test_apply_cast_type_string_to_float():
    ops = parse_ops([{"op": "cast_type", "field": "order_total", "target_type": "float"}])
    healed = apply_mapping({"order_total": "12.50"}, ops)
    assert healed["order_total"] == 12.5
    assert isinstance(healed["order_total"], float)


def test_apply_set_default_only_fills_missing_or_null():
    ops = parse_ops([{"op": "set_default", "field": "currency", "value": "USD"}])
    assert apply_mapping({}, ops)["currency"] == "USD"
    assert apply_mapping({"currency": None}, ops)["currency"] == "USD"
    assert apply_mapping({"currency": "EUR"}, ops)["currency"] == "EUR"


def test_apply_drop_field_is_noop_if_absent():
    ops = parse_ops([{"op": "drop_field", "field": "not_present"}])
    healed = apply_mapping({"a": 1}, ops)
    assert healed == {"a": 1}


def test_apply_const_map_uses_fallback_for_unknown_value():
    ops = parse_ops(
        [{"op": "const_map", "field": "pmt", "mapping": {"cc": "credit_card"}, "fallback": "unknown"}]
    )
    assert apply_mapping({"pmt": "cc"}, ops)["pmt"] == "credit_card"
    assert apply_mapping({"pmt": "bitcoin"}, ops)["pmt"] == "unknown"


def test_apply_mapping_does_not_mutate_input_record():
    original = {"order_amt": 12.5}
    ops = parse_ops([{"op": "rename_field", "source": "order_amt", "target": "order_total"}])
    apply_mapping(original, ops)
    assert original == {"order_amt": 12.5}  # untouched


def test_apply_mapping_composes_ops_in_order():
    raw = [
        {"op": "rename_field", "source": "amt", "target": "order_total"},
        {"op": "cast_type", "field": "order_total", "target_type": "float"},
        {"op": "set_default", "field": "currency", "value": "USD"},
    ]
    healed = apply_mapping({"amt": "9.99"}, parse_ops(raw))
    assert healed == {"order_total": 9.99, "currency": "USD"}
