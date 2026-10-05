"""
services/schema_healer/healer/contract_loader.py

Reads the same declarative YAML data contracts the sidecar validator (Paper 9)
already enforces — mounted read-only at CONTRACTS_DIR, matching
`sidecar_validator`'s existing `./contracts:/app/contracts:ro` volume.
schema_healer never invents its own notion of "correct schema"; it heals
*against* the contract that rejected the record in the first place.

Expected contract shape (matches the `orders_v1.yaml` example referenced in
README_week3.md — adjust here if your actual contract file uses different
key names):

    contract_name: orders_v1
    version: 1
    fields:
      - name: order_id
        type: string
        required: true
      - name: order_total
        type: float
        required: true
      - name: payment_method
        type: string
        required: true
        allowed_values: [credit_card, debit_card, paypal, gift_card]
      - name: currency
        type: string
        required: false
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class FieldContract:
    name: str
    type: str
    required: bool = True
    allowed_values: list | None = None


@dataclass
class DataContract:
    contract_name: str
    version: int
    fields: list[FieldContract]

    def field_names(self) -> set[str]:
        return {f.name for f in self.fields}

    def required_field_names(self) -> set[str]:
        return {f.name for f in self.fields if f.required}


class ContractNotFound(Exception):
    pass


class ContractStore:
    """Loads and caches contracts from a directory of YAML files. Re-reads
    from disk on cache miss only, so a contract file can be hot-edited
    (same operational pattern as the sidecar validator) without restarting
    this service — call `refresh()` after deploying a new contract if you
    want to pick it up without waiting for the next miss."""

    def __init__(self, contracts_dir: str | Path):
        self.contracts_dir = Path(contracts_dir)
        self._cache: dict[str, DataContract] = {}

    def refresh(self) -> None:
        self._cache.clear()

    def _load_all(self) -> None:
        if not self.contracts_dir.exists():
            return
        for path in self.contracts_dir.rglob("*.yaml"):
            try:
                raw = yaml.safe_load(path.read_text())
            except yaml.YAMLError:
                continue
            if not raw or "contract_name" not in raw:
                continue
            fields = [
                FieldContract(
                    name=f["name"],
                    type=f.get("type", "string"),
                    required=f.get("required", True),
                    allowed_values=f.get("allowed_values"),
                )
                for f in raw.get("fields", [])
            ]
            contract = DataContract(
                contract_name=raw["contract_name"],
                version=raw.get("version", 1),
                fields=fields,
            )
            self._cache[contract.contract_name] = contract

    def get(self, contract_name: str) -> DataContract:
        if contract_name not in self._cache:
            self._load_all()
        if contract_name not in self._cache:
            raise ContractNotFound(contract_name)
        return self._cache[contract_name]
