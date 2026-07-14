from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX = "uq_live_sim_intents_order_plan_id"
_MIGRATION_SAVEPOINT = "live_sim_order_plan_uniqueness_migration"


class LiveSimOrderPlanUniquenessError(RuntimeError):
    def __init__(self, reason_codes: list[str], audit: Mapping[str, Any]) -> None:
        normalized_reasons = tuple(sorted(set(reason_codes)))
        super().__init__(
            "live_sim order-plan uniqueness migration blocked: "
            + ",".join(normalized_reasons)
        )
        self.reason_codes = normalized_reasons
        self.audit = dict(audit)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "LIVE_SIM_ORDER_PLAN_UNIQUENESS_BLOCKED",
            "reason_codes": list(self.reason_codes),
            "duplicate_group_count": int(
                self.audit.get("duplicate_group_count") or 0
            ),
            "mismatch_count": int(self.audit.get("mismatch_count") or 0),
            "invalid_evidence_order_plan_id_count": int(
                self.audit.get("invalid_evidence_order_plan_id_count") or 0
            ),
        }


def ensure_live_sim_order_plan_uniqueness_schema(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(f"SAVEPOINT {_MIGRATION_SAVEPOINT}")
    try:
        if "order_plan_id" not in _table_columns(connection, "live_sim_intents"):
            connection.execute(
                "ALTER TABLE live_sim_intents ADD COLUMN order_plan_id TEXT"
            )

        audit = _audit_order_plan_ids(connection)
        blocking_reasons = _migration_blocking_reasons(audit)
        if blocking_reasons:
            raise LiveSimOrderPlanUniquenessError(blocking_reasons, audit)

        updates = audit.pop("updates")
        if updates:
            connection.executemany(
                """
                UPDATE live_sim_intents
                SET order_plan_id = ?
                WHERE live_sim_intent_id = ?
                """,
                updates,
            )
        connection.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX}
            ON live_sim_intents (order_plan_id)
            WHERE order_plan_id IS NOT NULL
            """
        )

        index_contract = _index_contract(connection)
        if not _valid_index_contract(index_contract):
            raise LiveSimOrderPlanUniquenessError(
                ["ORDER_PLAN_UNIQUE_INDEX_CONTRACT_INVALID"],
                index_contract,
            )

        post_migration_audit = _audit_order_plan_ids(connection)
        post_migration_reasons = _migration_blocking_reasons(
            post_migration_audit,
            include_missing_backfill=True,
        )
        if post_migration_reasons:
            raise LiveSimOrderPlanUniquenessError(
                post_migration_reasons,
                post_migration_audit,
            )
    except Exception:
        connection.execute(f"ROLLBACK TO {_MIGRATION_SAVEPOINT}")
        connection.execute(f"RELEASE {_MIGRATION_SAVEPOINT}")
        raise
    else:
        connection.execute(f"RELEASE {_MIGRATION_SAVEPOINT}")


def get_live_sim_order_plan_uniqueness_status(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    table_exists = _table_exists(connection, "live_sim_intents")
    column_exists = bool(
        table_exists
        and "order_plan_id" in _table_columns(connection, "live_sim_intents")
    )
    index_contract = _index_contract(connection) if table_exists else _empty_index_contract()

    if column_exists:
        audit = _audit_order_plan_ids(connection)
        audit.pop("updates", None)
    else:
        audit = _empty_audit()

    reason_codes: list[str] = []
    warning_codes: list[str] = []
    if not table_exists:
        reason_codes.append("LIVE_SIM_INTENTS_TABLE_MISSING")
    if not column_exists:
        reason_codes.append("ORDER_PLAN_ID_COLUMN_MISSING")
    if not _valid_index_contract(index_contract):
        reason_codes.append("ORDER_PLAN_UNIQUE_INDEX_CONTRACT_INVALID")
    if int(audit["duplicate_group_count"]) > 0:
        reason_codes.append("DUPLICATE_ORDER_PLAN_ID")
    if int(audit["mismatch_count"]) > 0:
        reason_codes.append("ORDER_PLAN_ID_EVIDENCE_MISMATCH")
    if int(audit["missing_backfill_count"]) > 0:
        reason_codes.append("ORDER_PLAN_ID_BACKFILL_MISSING")
    if int(audit["invalid_evidence_order_plan_id_count"]) > 0:
        reason_codes.append("ORDER_PLAN_ID_EVIDENCE_INVALID")
    if int(audit["invalid_evidence_json_count"]) > 0:
        warning_codes.append("INVALID_EVIDENCE_JSON_WITHOUT_ORDER_PLAN_ID")
    if int(audit["column_without_evidence_count"]) > 0:
        warning_codes.append("ORDER_PLAN_ID_COLUMN_WITHOUT_EVIDENCE")

    status = "FAIL" if reason_codes else "WARN" if warning_codes else "PASS"
    return {
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "warning_codes": sorted(set(warning_codes)),
        "table_exists": table_exists,
        "column_exists": column_exists,
        "unique_index_name": LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX,
        "unique_index_exists": bool(index_contract["exists"]),
        "unique_index_is_unique": bool(index_contract["unique"]),
        "unique_index_is_partial": bool(index_contract["partial"]),
        "lookup_strategy": "DIRECT_ORDER_PLAN_ID_INDEX_LOOKUP",
        **audit,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def _migration_blocking_reasons(
    audit: Mapping[str, Any],
    *,
    include_missing_backfill: bool = False,
) -> list[str]:
    reasons: list[str] = []
    if int(audit.get("duplicate_group_count") or 0) > 0:
        reasons.append("DUPLICATE_ORDER_PLAN_ID")
    if int(audit.get("mismatch_count") or 0) > 0:
        reasons.append("ORDER_PLAN_ID_EVIDENCE_MISMATCH")
    if int(audit.get("invalid_evidence_order_plan_id_count") or 0) > 0:
        reasons.append("ORDER_PLAN_ID_EVIDENCE_INVALID")
    if include_missing_backfill and int(audit.get("missing_backfill_count") or 0) > 0:
        reasons.append("ORDER_PLAN_ID_BACKFILL_MISSING")
    return reasons


def _audit_order_plan_ids(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT live_sim_intent_id, order_plan_id, evidence_json
        FROM live_sim_intents
        """
    ).fetchall()
    groups: dict[str, list[str]] = defaultdict(list)
    updates: list[tuple[str | None, str]] = []
    mismatch_count = 0
    missing_backfill_count = 0
    column_without_evidence_count = 0
    invalid_evidence_json_count = 0
    invalid_evidence_order_plan_id_count = 0
    json_order_plan_id_count = 0
    column_order_plan_id_count = 0

    for row in rows:
        intent_id = str(row[0])
        column_id = _normalize_order_plan_id(row[1])
        evidence_id, evidence_state = _evidence_order_plan_id(row[2])
        if column_id is not None:
            column_order_plan_id_count += 1
        if evidence_id is not None:
            json_order_plan_id_count += 1
        if evidence_state == "INVALID_JSON":
            invalid_evidence_json_count += 1
        elif evidence_state == "INVALID_ORDER_PLAN_ID":
            invalid_evidence_order_plan_id_count += 1

        if column_id is not None and evidence_id is not None and column_id != evidence_id:
            mismatch_count += 1
        if column_id is None and evidence_id is not None:
            missing_backfill_count += 1
        if column_id is not None and evidence_id is None:
            column_without_evidence_count += 1

        resolved_id = column_id or evidence_id
        if resolved_id is not None:
            groups[resolved_id].append(intent_id)
        if row[1] != resolved_id:
            updates.append((resolved_id, intent_id))

    duplicate_groups = {
        order_plan_id: intent_ids
        for order_plan_id, intent_ids in groups.items()
        if len(intent_ids) > 1
    }
    return {
        "intent_count": len(rows),
        "order_plan_intent_count": sum(len(intent_ids) for intent_ids in groups.values()),
        "column_order_plan_id_count": column_order_plan_id_count,
        "json_order_plan_id_count": json_order_plan_id_count,
        "duplicate_group_count": len(duplicate_groups),
        "duplicate_row_count": sum(len(ids) for ids in duplicate_groups.values()),
        "duplicate_order_plan_id_sample": sorted(duplicate_groups)[:10],
        "mismatch_count": mismatch_count,
        "missing_backfill_count": missing_backfill_count,
        "column_without_evidence_count": column_without_evidence_count,
        "invalid_evidence_json_count": invalid_evidence_json_count,
        "invalid_evidence_order_plan_id_count": invalid_evidence_order_plan_id_count,
        "updates": updates,
    }


def _evidence_order_plan_id(value: object) -> tuple[str | None, str | None]:
    if isinstance(value, Mapping):
        payload = value
    else:
        raw = str(value or "")
        try:
            loaded = json.loads(raw)
        except (TypeError, ValueError):
            state = "INVALID_ORDER_PLAN_ID" if "order_plan_id" in raw else "INVALID_JSON"
            return None, state
        if not isinstance(loaded, Mapping):
            return None, "INVALID_JSON"
        payload = loaded

    if "order_plan_id" not in payload or payload.get("order_plan_id") is None:
        return None, None
    raw_order_plan_id = payload.get("order_plan_id")
    if not isinstance(raw_order_plan_id, str):
        return None, "INVALID_ORDER_PLAN_ID"
    return _normalize_order_plan_id(raw_order_plan_id), None


def _normalize_order_plan_id(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _index_contract(connection: sqlite3.Connection) -> dict[str, Any]:
    for row in connection.execute("PRAGMA index_list(live_sim_intents)").fetchall():
        if str(row[1]) == LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX:
            return {
                "exists": True,
                "unique": bool(row[2]),
                "partial": bool(row[4]),
            }
    return _empty_index_contract()


def _empty_index_contract() -> dict[str, Any]:
    return {"exists": False, "unique": False, "partial": False}


def _valid_index_contract(contract: Mapping[str, Any]) -> bool:
    return bool(contract.get("exists") and contract.get("unique") and contract.get("partial"))


def _empty_audit() -> dict[str, Any]:
    return {
        "intent_count": 0,
        "order_plan_intent_count": 0,
        "column_order_plan_id_count": 0,
        "json_order_plan_id_count": 0,
        "duplicate_group_count": 0,
        "duplicate_row_count": 0,
        "duplicate_order_plan_id_sample": [],
        "mismatch_count": 0,
        "missing_backfill_count": 0,
        "column_without_evidence_count": 0,
        "invalid_evidence_json_count": 0,
        "invalid_evidence_order_plan_id_count": 0,
    }
