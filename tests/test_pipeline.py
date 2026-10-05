"""tests/test_pipeline.py — no infra needed; the LLM client is faked."""
from dataclasses import dataclass
from datetime import datetime, timezone

from generate_order_stream import make_schema_mismatch
from services.schema_healer.healer.circuit_guard import CircuitGuardConfig, new_state, record_failure
from services.schema_healer.healer.contract_loader import DataContract, FieldContract
from services.schema_healer.healer.pipeline import heal_one_record

NOW = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)

CONTRACT = DataContract(
    contract_name="orders_v1",
    version=1,
    fields=[
        FieldContract(name="order_id", type="string", required=True),
        FieldContract(name="order_total", type="float", required=True),
        FieldContract(
            name="payment_method",
            type="string",
            required=True,
            allowed_values=["card", "cod", "credit_card", "debit_card", "paypal", "gift_card"],
        ),
    ],
)


@dataclass
class FakeProposal:
    ops: list
    root_cause_category: str
    narrative: str
    confidence: float
    model_used: str = "fake-model"


class FakeLLMClient:
    def __init__(self, proposal: FakeProposal):
        self.proposal = proposal
        self.calls = 0

    def propose_mapping(self, contract_yaml_text, record, diagnostic_text):
        self.calls += 1
        return self.proposal


def _guard_config():
    return CircuitGuardConfig(failure_threshold=3, base_cooldown_seconds=60.0)


def test_successful_heal_publishes_to_validated_data_and_resets_guard():
    record = {"order_id": "o1", "amount_due": "42.50", "payment_method": "credit_card"}
    proposal = FakeProposal(
        ops=[
            {"op": "rename_field", "source": "amount_due", "target": "order_total"},
            {"op": "cast_type", "field": "order_total", "target_type": "float"},
        ],
        root_cause_category="field_renamed",
        narrative="Upstream renamed order_amt to order_total.",
        confidence=0.92,
    )
    outcome = heal_one_record(
        record=record,
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing required fields: ['order_total']",
        llm_client=FakeLLMClient(proposal),
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        now=NOW,
    )
    assert outcome.success is True
    assert outcome.target_topic == "validated-data"
    assert outcome.healed_record["order_total"] == 42.5
    assert outcome.new_guard_state.consecutive_failures == 0


def test_low_confidence_goes_to_dlq_and_increments_guard():
    proposal = FakeProposal(
        ops=[{"op": "rename_field", "source": "legacy_value", "target": "order_total"}],
        root_cause_category="unknown",
        narrative="Not sure about this one.",
        confidence=0.2,
    )
    outcome = heal_one_record(
        record={"order_id": "o1", "legacy_value": "42.50", "payment_method": "credit_card"},
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing required fields: ['order_total']",
        llm_client=FakeLLMClient(proposal),
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        min_confidence=0.6,
        now=NOW,
    )
    assert outcome.success is False
    assert outcome.target_topic == "dead-letter-queue"
    assert "low_confidence" in outcome.reason
    assert outcome.new_guard_state.consecutive_failures == 1


def test_empty_ops_list_is_treated_as_failure_even_with_high_confidence():
    # Guards against an LLM that reports high confidence but proposes nothing.
    proposal = FakeProposal(ops=[], root_cause_category="unknown", narrative="", confidence=0.99)
    outcome = heal_one_record(
        record={"order_id": "o1", "payment_method": "credit_card"},
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing required fields: ['order_total']",
        llm_client=FakeLLMClient(proposal),
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        now=NOW,
    )
    assert outcome.success is False


def test_llm_proposing_ops_outside_vocabulary_is_rejected_not_executed():
    proposal = FakeProposal(
        ops=[{"op": "exec_python", "code": "import os; os.system('echo pwned')"}],
        root_cause_category="unknown",
        narrative="",
        confidence=0.99,
    )
    outcome = heal_one_record(
        record={"order_id": "o1", "payment_method": "credit_card"},
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing required fields: ['order_total']",
        llm_client=FakeLLMClient(proposal),
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        now=NOW,
    )
    assert outcome.success is False
    assert "mapping_rejected" in outcome.reason


def test_mapping_that_does_not_actually_fix_the_record_fails_post_heal_check():
    # Ops apply cleanly but don't address the real problem (payment_method
    # left as an out-of-enum value) — post-heal re-validation must catch this.
    proposal = FakeProposal(
        ops=[{"op": "set_default", "field": "unrelated_field", "value": "x"}],
        root_cause_category="unknown",
        narrative="",
        confidence=0.9,
    )
    outcome = heal_one_record(
        record={"order_id": "o1", "order_total": 10.0, "payment_method": "bitcoin"},
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="invalid enum values: ['payment_method=bitcoin']",
        llm_client=FakeLLMClient(proposal),
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        now=NOW,
    )
    assert outcome.success is False
    assert "post_heal_revalidation_failed" in outcome.reason


def test_paused_guard_skips_the_llm_call_entirely():
    config = _guard_config()
    state = new_state("orders_v1")
    for _ in range(config.failure_threshold):
        state = record_failure(state, NOW, config)
    assert state.paused_until is not None

    proposal = FakeProposal(ops=[], root_cause_category="unknown", narrative="", confidence=0.0)
    fake_client = FakeLLMClient(proposal)

    outcome = heal_one_record(
        record={"order_id": "o1"},
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing required fields: ['order_total', 'payment_method']",
        llm_client=fake_client,
        guard_state=state,
        guard_config=config,
        now=NOW,  # still inside the cooldown window
    )
    assert outcome.success is False
    assert outcome.reason.startswith("healer_paused")
    assert fake_client.calls == 0  # the whole point: no LLM call while paused


def test_extra_unrecognized_field_does_not_block_a_heal_by_default():
    # Full-stack validation finding (2026-09-23): an extra, unrecognized
    # field alone used to force strict_extra_fields=True behavior, requiring
    # the LLM to also emit a correct drop_field op just to pass — on top of
    # whatever the real mismatch needed. Default is lenient now: an
    # unrecognized field is diagnostic-only, not blocking, unless the
    # caller opts into strict_extra_fields=True.
    record = {
        "order_id": "o1",
        "order_amt": "42.50",
        "payment_method": "credit_card",
        "loyalty_tier": "gold",  # not in CONTRACT at all
    }
    proposal = FakeProposal(
        ops=[
            {"op": "rename_field", "source": "order_amt", "target": "order_total"},
            {"op": "cast_type", "field": "order_total", "target_type": "float"},
        ],
        root_cause_category="field_renamed",
        narrative="renamed only — did not touch the extra loyalty_tier field",
        confidence=0.9,
    )
    outcome = heal_one_record(
        record=record,
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing required fields: ['order_total']",
        llm_client=FakeLLMClient(proposal),
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        strict_extra_fields=False,  # default
        now=NOW,
    )
    assert outcome.success is True
    assert outcome.healed_record["loyalty_tier"] == "gold"  # left untouched, and that's fine


def test_extra_unrecognized_field_blocks_a_heal_when_strict_extra_fields_true():
    # Same record and mapping as above, but with the stricter opt-in — now
    # the untouched loyalty_tier field must fail post-heal re-validation.
    record = {
        "order_id": "o1",
        "order_amt": "42.50",
        "payment_method": "credit_card",
        "loyalty_tier": "gold",
    }
    proposal = FakeProposal(
        ops=[
            {"op": "rename_field", "source": "order_amt", "target": "order_total"},
            {"op": "cast_type", "field": "order_total", "target_type": "float"},
        ],
        root_cause_category="field_renamed",
        narrative="",
        confidence=0.9,
    )
    outcome = heal_one_record(
        record=record,
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing required fields: ['order_total']",
        llm_client=FakeLLMClient(proposal),
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        strict_extra_fields=True,
        now=NOW,
    )
    assert outcome.success is False
    assert "post_heal_revalidation_failed" in outcome.reason


def test_generator_schema_mismatch_uses_deterministic_fallback_when_llm_fails():
    record = make_schema_mismatch()
    llm_client = FakeLLMClient(
        FakeProposal(
            ops=[],
            root_cause_category="unknown",
            narrative="LLM unavailable",
            confidence=0.0,
            model_used=None,
        )
    )

    outcome = heal_one_record(
        record=record,
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing order_total; unexpected order_amt",
        llm_client=llm_client,
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        now=NOW,
    )

    assert llm_client.calls == 0
    assert outcome.success is True
    assert outcome.target_topic == "validated-data"
    assert outcome.reason == "healed_and_revalidated_with_deterministic_rename"
    assert outcome.healed_record["order_total"] == float(record["order_amt"])
    assert "order_amt" not in outcome.healed_record
    assert outcome.healed_record["loyalty_tier"] == record["loyalty_tier"]


def test_unique_alias_uses_deterministic_mapping_without_waiting_for_llm():
    record = make_schema_mismatch()
    llm_client = FakeLLMClient(
        FakeProposal(
            ops=[
                {"op": "rename_field", "source": "order_amt", "target": "order_amount"},
                {"op": "cast_type", "field": "order_amount", "target_type": "float"},
            ],
            root_cause_category="field_renamed",
            narrative="Renamed to order_amount.",
            confidence=0.99,
        )
    )

    outcome = heal_one_record(
        record=record,
        contract=CONTRACT,
        contract_yaml_text="contract_name: orders_v1",
        diagnostic_text="missing order_total; unexpected order_amt",
        llm_client=llm_client,
        guard_state=new_state("orders_v1"),
        guard_config=_guard_config(),
        now=NOW,
    )

    assert llm_client.calls == 0
    assert outcome.success is True
    assert outcome.reason == "healed_and_revalidated_with_deterministic_rename"
    assert outcome.healed_record["order_total"] == float(record["order_amt"])
    assert outcome.model_used is None
