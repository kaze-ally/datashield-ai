"""
services/schema_healer/healer/persistence.py

Postgres-backed persistence for schema_healing_reports and
schema_healer_circuit_state (DDL: postgres/init/005_schema_healing.sql).

Explicitly casts to native Python `bool`/`float`/`str` before every insert.
This project has already been bitten once by a numpy.bool_ -> psycopg2
adapter mismatch (see README_week3.md's "silent-failure class of bug"
note on the Isolation Forest); schema_healer's confidence/success values
don't originate from numpy here, but the cast is cheap insurance against
the same class of bug if a future caller feeds this module a numpy/pandas
scalar.
"""
from __future__ import annotations

import json
from datetime import datetime

import psycopg2
import psycopg2.extras

from .circuit_guard import CircuitGuardState
from .pipeline import HealingOutcome


class SchemaHealerRepository:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def _connect(self):
        return psycopg2.connect(self.dsn)

    def save_report(self, report_id: str, contract_name: str, outcome: HealingOutcome) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO schema_healing_reports
                    (report_id, contract_name, success, reason, root_cause_category,
                     narrative, confidence, model_used, ops_applied)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    report_id,
                    str(contract_name),
                    bool(outcome.success),
                    str(outcome.reason),
                    str(outcome.root_cause_category),
                    str(outcome.narrative),
                    float(outcome.confidence),
                    outcome.model_used,
                    json.dumps(outcome.ops_applied),
                ),
            )

    def load_guard_state(self, contract_name: str) -> CircuitGuardState:
        with self._connect() as conn, conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(
                "SELECT contract_name, consecutive_failures, paused_until, trip_count "
                "FROM schema_healer_circuit_state WHERE contract_name = %s",
                (contract_name,),
            )
            row = cur.fetchone()
        if row is None:
            return CircuitGuardState(contract_name=contract_name)
        return CircuitGuardState(
            contract_name=row["contract_name"],
            consecutive_failures=row["consecutive_failures"],
            paused_until=row["paused_until"],
            trip_count=row["trip_count"],
        )

    def save_guard_state(self, state: CircuitGuardState) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO schema_healer_circuit_state
                    (contract_name, consecutive_failures, paused_until, trip_count, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (contract_name) DO UPDATE SET
                    consecutive_failures = EXCLUDED.consecutive_failures,
                    paused_until = EXCLUDED.paused_until,
                    trip_count = EXCLUDED.trip_count,
                    updated_at = now()
                """,
                (
                    state.contract_name,
                    int(state.consecutive_failures),
                    state.paused_until,
                    int(state.trip_count),
                ),
            )

    def recent_reports(self, contract_name: str, limit: int = 10) -> list[dict]:
        with self._connect() as conn, conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(
                """
                SELECT report_id, success, reason, root_cause_category, narrative,
                       confidence, model_used, ops_applied, created_at
                FROM schema_healing_reports
                WHERE contract_name = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (contract_name, limit),
            )
            return [dict(r) for r in cur.fetchall()]
