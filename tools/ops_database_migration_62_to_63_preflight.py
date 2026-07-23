from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from storage.sqlite import (  # noqa: E402
    APP_NAME,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_INDEXES,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TRIGGERS,
    SCHEMA_VERSION,
    initialize_database,
    migrate_schema_62_to_63,
)
from tools import ops_database_migration_preflight as snapshot_tool  # noqa: E402

SOURCE_SCHEMA = "62"
TARGET_SCHEMA = "63"
PREFLIGHT_CONTRACT_VERSION = "exact-62-to-63-fence-events-v1"
EXACT_MIGRATION_METHOD = "exact_storage_migrate_schema_62_to_63"
SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")

TARGET_OBJECT_TYPES = {
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE: "table",
    **{name: "index" for name in GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_INDEXES},
    **{name: "trigger" for name in GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TRIGGERS},
}
TARGET_COLUMNS = (
    "fence_event_id",
    "request_id",
    "request_hash",
    "command_id",
    "command_alias",
    "sequence_no",
    "action",
    "supersedes_fence_event_id",
    "resolution_id",
    "resolution_request_hash",
    "source_boundary_fingerprint",
    "approval_id",
    "approval_trade_date",
    "approval_sha256",
    "evidence_sha256",
    "database_identity_sha256",
    "expected_app_name",
    "expected_schema_version",
    "expected_gateway_command_total_count",
    "expected_order_command_count",
    "expected_gateway_command_state_fingerprint",
    "reason_code",
    "operator_id",
    "created_at",
    "live_sim_only",
    "live_real_allowed",
)
TARGET_COLUMN_CONTRACTS = (
    ("fence_event_id", "TEXT", False, None, 1, 0),
    ("request_id", "TEXT", True, None, 0, 0),
    ("request_hash", "TEXT", True, None, 0, 0),
    ("command_id", "TEXT", True, None, 0, 0),
    ("command_alias", "TEXT", True, None, 0, 0),
    ("sequence_no", "INTEGER", True, None, 0, 0),
    ("action", "TEXT", True, None, 0, 0),
    ("supersedes_fence_event_id", "TEXT", False, None, 0, 0),
    ("resolution_id", "TEXT", True, None, 0, 0),
    ("resolution_request_hash", "TEXT", True, None, 0, 0),
    ("source_boundary_fingerprint", "TEXT", True, None, 0, 0),
    ("approval_id", "TEXT", True, None, 0, 0),
    ("approval_trade_date", "TEXT", True, None, 0, 0),
    ("approval_sha256", "TEXT", True, None, 0, 0),
    ("evidence_sha256", "TEXT", True, None, 0, 0),
    ("database_identity_sha256", "TEXT", True, None, 0, 0),
    ("expected_app_name", "TEXT", True, None, 0, 0),
    ("expected_schema_version", "INTEGER", True, None, 0, 0),
    ("expected_gateway_command_total_count", "INTEGER", True, None, 0, 0),
    ("expected_order_command_count", "INTEGER", True, None, 0, 0),
    ("expected_gateway_command_state_fingerprint", "TEXT", True, None, 0, 0),
    ("reason_code", "TEXT", True, None, 0, 0),
    ("operator_id", "TEXT", True, None, 0, 0),
    ("created_at", "TEXT", True, None, 0, 0),
    ("live_sim_only", "INTEGER", True, "1", 0, 0),
    ("live_real_allowed", "INTEGER", True, "0", 0, 0),
)
TARGET_INDEX_CONTRACTS = {
    "idx_gateway_order_boundary_fence_events_created": {
        "columns": ("created_at", "fence_event_id"),
        "unique": False,
        "descending": (True, True),
    },
    "uq_gateway_order_boundary_fence_events_request_id": {
        "columns": ("request_id",),
        "unique": True,
        "descending": (False,),
    },
    "uq_gateway_order_boundary_fence_events_command_sequence": {
        "columns": ("command_id", "sequence_no"),
        "unique": True,
        "descending": (False, False),
    },
}
TARGET_TRIGGER_CONTRACTS = {
    "trg_gateway_order_boundary_fence_events_no_update": "UPDATE",
    "trg_gateway_order_boundary_fence_events_no_delete": "DELETE",
}
TARGET_FOREIGN_KEY_CONTRACTS = tuple(
    sorted(
        {
            (
                "command_id",
                "gateway_order_broker_boundaries",
                "command_id",
                "NO ACTION",
                "NO ACTION",
                "NONE",
            ),
            (
                "resolution_id",
                "gateway_order_broker_boundary_resolutions",
                "resolution_id",
                "NO ACTION",
                "NO ACTION",
                "NONE",
            ),
            (
                "supersedes_fence_event_id",
                GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,
                "fence_event_id",
                "NO ACTION",
                "NO ACTION",
                "NONE",
            ),
        }
    )
)
TARGET_TABLE_SQL_TOKENS = (
    "CHECK (SEQUENCE_NO > 0)",
    "CHECK (ACTION IN ('RELEASE', 'REINSTATE'))",
    "CHECK (LENGTH(REQUEST_HASH) = 64)",
    "CHECK (LENGTH(RESOLUTION_REQUEST_HASH) = 64)",
    "CHECK (LENGTH(SOURCE_BOUNDARY_FINGERPRINT) = 64)",
    "CHECK (LENGTH(APPROVAL_SHA256) = 64)",
    "CHECK (LENGTH(EVIDENCE_SHA256) = 64)",
    "CHECK (LENGTH(DATABASE_IDENTITY_SHA256) = 64)",
    "CHECK (LENGTH(EXPECTED_GATEWAY_COMMAND_STATE_FINGERPRINT) = 64)",
    "CHECK (LENGTH(APPROVAL_TRADE_DATE) = 10)",
    "CHECK (EXPECTED_APP_NAME = 'SUSEOK-TRADER-V2')",
    "CHECK (EXPECTED_SCHEMA_VERSION = 63)",
    "CHECK (EXPECTED_GATEWAY_COMMAND_TOTAL_COUNT >= 0)",
    (
        "CHECK (EXPECTED_ORDER_COMMAND_COUNT >= 0 "
        "AND EXPECTED_ORDER_COMMAND_COUNT "
        "<= EXPECTED_GATEWAY_COMMAND_TOTAL_COUNT)"
    ),
    "CHECK (LIVE_SIM_ONLY = 1)",
    "CHECK (LIVE_REAL_ALLOWED = 0)",
    "REFERENCES GATEWAY_ORDER_BROKER_BOUNDARIES",
    "REFERENCES GATEWAY_ORDER_BROKER_BOUNDARY_RESOLUTIONS",
    "REFERENCES GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENTS",
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Clone a quiescent schema-62 SQLite database read-only and validate "
            "the exact additive schema 62 -> 63 fence-event migration."
        )
    )
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--clone-db", required=True)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "database_migration_62_to_63_preflight"),
    )
    args = parser.parse_args()

    report = run_preflight(
        source_db=Path(args.source_db),
        clone_db=Path(args.clone_db),
        out_dir=Path(args.out_dir),
    )
    verdict = _mapping(report.get("verdict"))
    print(
        "database migration 62->63 preflight: "
        f"{verdict.get('status', 'UNKNOWN')} "
        f"source_unchanged={str(verdict.get('source_files_unchanged')).lower()} "
        f"raw={_mapping(report.get('report_paths')).get('raw_json')}",
        flush=True,
    )
    return 0 if verdict.get("status") == "PASS" else 2


def run_preflight(
    *,
    source_db: Path,
    clone_db: Path,
    out_dir: Path,
) -> dict[str, Any]:
    if str(SCHEMA_VERSION) != TARGET_SCHEMA:
        raise RuntimeError(
            f"schema 62 -> 63 preflight requires code target 63; actual={SCHEMA_VERSION}"
        )

    source = source_db.expanduser().resolve()
    clone = clone_db.expanduser().resolve()
    _validate_paths(source=source, clone=clone)
    disk = snapshot_tool._disk_space_contract(source=source, clone=clone)
    if disk.get("sufficient") is not True:
        raise RuntimeError("insufficient free space for schema 62 -> 63 clone")

    source_files_before = snapshot_tool._file_fingerprints(source)
    source_started = time.perf_counter()
    source_connection = snapshot_tool._open_read_only(source)
    try:
        source_quick_check = _quick_check_connection(source_connection)
        source_snapshot = _database_snapshot(source_connection)
        _assert_exact_source(source_snapshot)

        clone.parent.mkdir(parents=True, exist_ok=True)
        clone_connection = sqlite3.connect(clone, timeout=60.0)
        try:
            source_connection.backup(clone_connection, pages=65_536, sleep=0.05)
        finally:
            clone_connection.close()
    finally:
        source_connection.close()
    source_elapsed_sec = time.perf_counter() - source_started

    source_files_after = snapshot_tool._file_fingerprints(source)
    clone_before = _snapshot_path(clone)
    clone_before_quick_check = _quick_check_path(clone)

    migration_started = time.perf_counter()
    migration_connection = sqlite3.connect(clone, timeout=60.0, isolation_level=None)
    migration_connection.row_factory = sqlite3.Row
    migration_connection.execute("PRAGMA foreign_keys=ON")
    try:
        migration_connection.execute("BEGIN EXCLUSIVE")
        migrate_schema_62_to_63(migration_connection)
        migration_connection.commit()
    except Exception:
        if migration_connection.in_transaction:
            migration_connection.rollback()
        raise
    finally:
        migration_connection.close()
    migration_elapsed_sec = time.perf_counter() - migration_started

    clone_after_exact = _snapshot_path(clone)
    exact_quick_check = _quick_check_path(clone)

    idempotent_started = time.perf_counter()
    idempotent = initialize_database(clone)
    idempotent.close()
    idempotent_elapsed_sec = time.perf_counter() - idempotent_started
    clone_after_initializer = _snapshot_path(clone)
    final_quick_check = _quick_check_path(clone)

    report: dict[str, Any] = {
        "generated_at": _now(),
        "preflight_contract_version": PREFLIGHT_CONTRACT_VERSION,
        "required_source_schema": SOURCE_SCHEMA,
        "target_schema_version": TARGET_SCHEMA,
        "source": {
            "path": str(source),
            "files_before": source_files_before,
            "files_after": source_files_after,
            "snapshot": source_snapshot,
            "quick_check": source_quick_check,
            "opened_read_only": True,
            "query_only": True,
            "immutable": True,
            "sidecar_count_before": 0,
            "sidecar_count_after": _sidecar_count(source),
            "elapsed_sec": round(source_elapsed_sec, 6),
        },
        "clone": {
            "path": str(clone),
            "disk_space": disk,
            "migration_method": EXACT_MIGRATION_METHOD,
            "before_migration": clone_before,
            "after_exact_migration": clone_after_exact,
            "after_initializer": clone_after_initializer,
            "before_quick_check": clone_before_quick_check,
            "exact_quick_check": exact_quick_check,
            "quick_check": final_quick_check,
            "migration_elapsed_sec": round(migration_elapsed_sec, 6),
            "idempotent_elapsed_sec": round(idempotent_elapsed_sec, 6),
        },
    }
    report["verdict"] = evaluate_report(report)
    paths = _write_report(report, out_dir=out_dir)
    report["report_paths"] = {name: str(path) for name, path in paths.items()}
    return report


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    source = _mapping(report.get("source"))
    clone = _mapping(report.get("clone"))
    source_snapshot = _mapping(source.get("snapshot"))
    before = _mapping(clone.get("before_migration"))
    after = _mapping(clone.get("after_exact_migration"))
    after_initializer = _mapping(clone.get("after_initializer"))

    if report.get("preflight_contract_version") != PREFLIGHT_CONTRACT_VERSION:
        failures.append("PREFLIGHT_CONTRACT_VERSION_INVALID")
    if report.get("required_source_schema") != SOURCE_SCHEMA:
        failures.append("SOURCE_SCHEMA_BINDING_INVALID")
    if report.get("target_schema_version") != TARGET_SCHEMA:
        failures.append("TARGET_SCHEMA_BINDING_INVALID")
    if clone.get("migration_method") != EXACT_MIGRATION_METHOD:
        failures.append("MIGRATION_METHOD_INVALID")

    source_identity = _mapping(source_snapshot.get("app_identity"))
    before_identity = _mapping(before.get("app_identity"))
    after_identity = _mapping(after.get("app_identity"))
    if source_snapshot.get("schema_version") != SOURCE_SCHEMA:
        failures.append("SOURCE_SCHEMA_MISMATCH")
    if not _identity_matches(source_identity, schema=SOURCE_SCHEMA):
        failures.append("SOURCE_APPLICATION_IDENTITY_INVALID")
    if before.get("schema_version") != SOURCE_SCHEMA:
        failures.append("CLONE_SOURCE_SCHEMA_MISMATCH")
    if before_identity != source_identity:
        failures.append("CLONE_SOURCE_IDENTITY_MISMATCH")
    if after.get("schema_version") != TARGET_SCHEMA:
        failures.append("TARGET_SCHEMA_MISMATCH")
    if not _identity_matches(after_identity, schema=TARGET_SCHEMA):
        failures.append("TARGET_APPLICATION_IDENTITY_INVALID")

    source_objects = _mapping(source_snapshot.get("migration_objects"))
    if any(value is not None for value in source_objects.values()):
        failures.append("SOURCE_TARGET_OBJECT_PRESENT")
    target_objects = _mapping(after.get("migration_objects"))
    if target_objects != TARGET_OBJECT_TYPES:
        failures.append("TARGET_OBJECT_SET_INVALID")
    target_contract = _mapping(after.get("fence_event_contract"))
    if target_contract.get("ready") is not True:
        failures.append("TARGET_FENCE_EVENT_CONTRACT_INVALID")

    source_tables = _mapping(source_snapshot.get("table_content"))
    before_tables = _mapping(before.get("table_content"))
    after_tables = _mapping(after.get("table_content"))
    backup_tables_preserved = before_tables == source_tables
    if not backup_tables_preserved:
        failures.append("CLONE_BACKUP_TABLE_CONTENT_MISMATCH")
    expected_target_tables = {*source_tables, GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE}
    target_table_set_exact = set(after_tables) == expected_target_tables
    if not target_table_set_exact:
        failures.append("TARGET_TABLE_SET_INVALID")
    changed_existing_tables = sorted(
        name for name, fingerprint in source_tables.items() if after_tables.get(name) != fingerprint
    )
    if changed_existing_tables:
        failures.append("EXISTING_TABLE_CONTENT_CHANGED")
    ledger = _mapping(after_tables.get(GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE))
    target_ledger_empty = ledger.get("row_count") == 0
    if not target_ledger_empty:
        failures.append("TARGET_FENCE_EVENT_LEDGER_NOT_EMPTY")

    outbox_preserved = bool(
        source_snapshot.get("projection_outbox")
        == before.get("projection_outbox")
        == after.get("projection_outbox")
    )
    if not outbox_preserved:
        failures.append("PROJECTION_OUTBOX_CHANGED")
    sqlite_sequence_preserved = bool(
        source_snapshot.get("sqlite_sequence")
        == before.get("sqlite_sequence")
        == after.get("sqlite_sequence")
    )
    if not sqlite_sequence_preserved:
        failures.append("SQLITE_SEQUENCE_CHANGED")
    order_state_preserved = bool(
        source_snapshot.get("order_state") == before.get("order_state") == after.get("order_state")
    )
    if not order_state_preserved:
        failures.append("ORDER_STATE_CHANGED")

    source_files_unchanged = source.get("files_before") == source.get("files_after")
    if not source_files_unchanged:
        failures.append("SOURCE_DATA_FILE_CHANGED")
    if source.get("sidecar_count_before") != 0 or source.get("sidecar_count_after") != 0:
        failures.append("SOURCE_SIDECAR_PRESENT")
    if (
        source.get("opened_read_only") is not True
        or source.get("query_only") is not True
        or source.get("immutable") is not True
    ):
        failures.append("SOURCE_READ_ONLY_CONTRACT_INVALID")
    if source.get("quick_check") != ["ok"]:
        failures.append("SOURCE_QUICK_CHECK_FAILED")
    if clone.get("before_quick_check") != ["ok"]:
        failures.append("CLONE_BACKUP_QUICK_CHECK_FAILED")
    if clone.get("exact_quick_check") != ["ok"]:
        failures.append("EXACT_TARGET_QUICK_CHECK_FAILED")
    if clone.get("quick_check") != ["ok"]:
        failures.append("FINAL_TARGET_QUICK_CHECK_FAILED")

    initializer_noop = bool(after and after_initializer and after_initializer == after)
    if not initializer_noop:
        failures.append("TARGET_INITIALIZER_CHANGED_DATABASE")

    return {
        "status": "FAIL" if failures else "PASS",
        "failures": failures,
        "source_schema_version": source_snapshot.get("schema_version"),
        "target_schema_version": after.get("schema_version"),
        "source_files_unchanged": source_files_unchanged,
        "source_data_files_unchanged": source_files_unchanged,
        "source_original_write_detected": not source_files_unchanged,
        "operating_database_mutated": not source_files_unchanged,
        "backup_tables_preserved": backup_tables_preserved,
        "backup_table_content_preserved": backup_tables_preserved,
        "target_table_set_exact": target_table_set_exact,
        "existing_table_content_preserved": not changed_existing_tables,
        "migration_table_content_preserved": not changed_existing_tables,
        "changed_existing_tables": changed_existing_tables,
        "order_state_preserved": order_state_preserved,
        "projection_outbox_preserved": outbox_preserved,
        "sqlite_sequence_preserved": sqlite_sequence_preserved,
        "target_objects_absent_at_source": not any(
            value is not None for value in source_objects.values()
        ),
        "target_objects_exact": target_objects == TARGET_OBJECT_TYPES,
        "target_contract_valid": target_contract.get("ready") is True,
        "target_ledger_empty": target_ledger_empty,
        "initializer_noop": initializer_noop,
        "quick_checks_ok": bool(
            source.get("quick_check") == ["ok"]
            and clone.get("before_quick_check") == ["ok"]
            and clone.get("exact_quick_check") == ["ok"]
            and clone.get("quick_check") == ["ok"]
        ),
        "quick_check": clone.get("quick_check"),
        "exact_quick_check": clone.get("exact_quick_check"),
    }


def _assert_exact_source(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema_version") != SOURCE_SCHEMA:
        raise RuntimeError(
            "schema 62 -> 63 preflight source CAS mismatch: "
            f"expected=62 actual={snapshot.get('schema_version')!r}"
        )
    identity = _mapping(snapshot.get("app_identity"))
    if not _identity_matches(identity, schema=SOURCE_SCHEMA):
        raise RuntimeError("schema 62 -> 63 preflight application identity mismatch")
    objects = _mapping(snapshot.get("migration_objects"))
    present = sorted(name for name, value in objects.items() if value is not None)
    if present:
        raise RuntimeError(
            "schema 62 -> 63 preflight target objects must be absent: " + ",".join(present)
        )


def _database_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    snapshot = snapshot_tool._database_snapshot(connection)
    table_content = _mapping(snapshot.get("table_content"))
    snapshot["app_identity"] = _app_identity(connection)
    snapshot["migration_objects"] = _migration_objects(connection)
    snapshot["fence_event_contract"] = _fence_event_contract(connection)
    snapshot["order_state"] = {
        name: fingerprint
        for name, fingerprint in table_content.items()
        if _is_order_state_table(name)
    }
    return snapshot


def _snapshot_path(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(path, timeout=60.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        return _database_snapshot(connection)
    finally:
        connection.close()


def _quick_check_path(path: Path) -> list[str]:
    connection = sqlite3.connect(path, timeout=60.0)
    try:
        connection.execute("PRAGMA query_only=ON")
        return _quick_check_connection(connection)
    finally:
        connection.close()


def _quick_check_connection(connection: sqlite3.Connection) -> list[str]:
    return [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]


def _app_identity(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    result: dict[str, Any] = {
        "schema_row_count": 0,
        "schema_version": None,
        "app_row_count": 0,
        "app_name": None,
    }
    if "app_metadata" not in tables:
        return result
    schema_rows = connection.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchall()
    app_rows = connection.execute(
        "SELECT value FROM app_metadata WHERE key = 'app_name'"
    ).fetchall()
    result.update(
        {
            "schema_row_count": len(schema_rows),
            "schema_version": (None if len(schema_rows) != 1 else str(schema_rows[0][0])),
            "app_row_count": len(app_rows),
            "app_name": None if len(app_rows) != 1 else str(app_rows[0][0]),
        }
    )
    return result


def _identity_matches(identity: Mapping[str, Any], *, schema: str) -> bool:
    return bool(
        identity.get("schema_row_count") == 1
        and identity.get("schema_version") == schema
        and identity.get("app_row_count") == 1
        and identity.get("app_name") == APP_NAME
    )


def _migration_objects(connection: sqlite3.Connection) -> dict[str, str | None]:
    rows = connection.execute(
        "SELECT name, type FROM sqlite_master WHERE name IN ("
        + ",".join("?" for _ in TARGET_OBJECT_TYPES)
        + ")",
        tuple(sorted(TARGET_OBJECT_TYPES)),
    ).fetchall()
    found = {str(row[0]): str(row[1]) for row in rows}
    return {name: found.get(name) for name in sorted(TARGET_OBJECT_TYPES)}


def _fence_event_contract(connection: sqlite3.Connection) -> dict[str, Any]:
    objects = _migration_objects(connection)
    if objects.get(GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE) != "table":
        return {
            "ready": False,
            "columns_exact": False,
            "table_constraints_valid": False,
            "foreign_keys_exact": False,
            "index_set_exact": False,
            "trigger_set_exact": False,
            "indexes": {},
            "triggers": {},
        }

    column_rows = connection.execute(
        f"PRAGMA table_xinfo({GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE})"
    ).fetchall()
    columns = tuple(str(row[1]) for row in column_rows)
    column_contracts = tuple(
        (
            str(row[1]),
            str(row[2]).upper(),
            bool(row[3]),
            None if row[4] is None else str(row[4]),
            int(row[5]),
            int(row[6]),
        )
        for row in column_rows
    )
    table_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,),
    ).fetchone()
    table_sql = "" if table_row is None else str(table_row[0] or "").upper()
    compact_table_sql = re.sub(r"\s+", "", table_sql)
    table_constraints_valid = all(
        re.sub(r"\s+", "", token) in compact_table_sql for token in TARGET_TABLE_SQL_TOKENS
    )

    foreign_keys = tuple(
        sorted(
            (
                str(row[3]),
                str(row[2]),
                str(row[4]),
                str(row[5]).upper(),
                str(row[6]).upper(),
                str(row[7]).upper(),
            )
            for row in connection.execute(
                f"PRAGMA foreign_key_list({GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE})"
            )
        )
    )
    foreign_keys_exact = foreign_keys == TARGET_FOREIGN_KEY_CONTRACTS

    all_index_rows = connection.execute(
        f"PRAGMA index_list({GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE})"
    ).fetchall()
    explicit_index_names = {str(row[1]) for row in all_index_rows if str(row[3]).lower() == "c"}
    automatic_index_contracts: set[tuple[str, bool, bool, tuple[str, ...], tuple[bool, ...]]] = (
        set()
    )
    for row in all_index_rows:
        if str(row[3]).lower() == "c":
            continue
        name = str(row[1])
        key_rows = [
            key_row
            for key_row in connection.execute(f"PRAGMA index_xinfo({name})")
            if int(key_row[5] or 0) == 1 and int(key_row[1]) >= 0
        ]
        key_rows.sort(key=lambda key_row: int(key_row[0]))
        automatic_index_contracts.add(
            (
                str(row[3]).lower(),
                bool(row[2]),
                bool(row[4]),
                tuple(str(key_row[2]) for key_row in key_rows),
                tuple(bool(key_row[3]) for key_row in key_rows),
            )
        )
    automatic_indexes_exact = automatic_index_contracts == {
        ("pk", True, False, ("fence_event_id",), (False,)),
        ("u", True, False, ("request_id",), (False,)),
    }
    index_set_exact = bool(
        explicit_index_names == set(TARGET_INDEX_CONTRACTS) and automatic_indexes_exact
    )

    index_contracts: dict[str, bool] = {}
    for name, expected in TARGET_INDEX_CONTRACTS.items():
        index_row = next((row for row in all_index_rows if str(row[1]) == name), None)
        if index_row is None:
            index_contracts[name] = False
            continue
        key_rows = [
            row
            for row in connection.execute(f"PRAGMA index_xinfo({name})")
            if int(row[5] or 0) == 1 and int(row[1]) >= 0
        ]
        key_rows.sort(key=lambda row: int(row[0]))
        index_contracts[name] = bool(
            bool(index_row[2]) is bool(expected["unique"])
            and not bool(index_row[4])
            and tuple(str(row[2]) for row in key_rows) == expected["columns"]
            and tuple(bool(row[3]) for row in key_rows) == expected["descending"]
        )

    trigger_contracts: dict[str, bool] = {}
    table_trigger_rows = connection.execute(
        "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?",
        (GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,),
    ).fetchall()
    table_trigger_sql = {str(row[0]): str(row[1] or "") for row in table_trigger_rows}
    trigger_set_exact = set(table_trigger_sql) == set(TARGET_TRIGGER_CONTRACTS)
    for name, operation in TARGET_TRIGGER_CONTRACTS.items():
        sql = table_trigger_sql.get(name, "")
        compact = re.sub(r"\s+", "", sql.upper()).replace("IFNOTEXISTS", "").rstrip(";")
        expected = (
            f"CREATETRIGGER{name.upper()}BEFORE{operation}ON"
            f"{GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE.upper()}"
            "BEGINSELECTRAISE(ABORT,"
            "'GATEWAYORDER-BOUNDARYFENCEEVENTSAREAPPEND-ONLY');END"
        )
        trigger_contracts[name] = compact == expected

    ready = bool(
        columns == TARGET_COLUMNS
        and column_contracts == TARGET_COLUMN_CONTRACTS
        and table_constraints_valid
        and foreign_keys_exact
        and index_set_exact
        and trigger_set_exact
        and all(index_contracts.values())
        and all(trigger_contracts.values())
        and objects == TARGET_OBJECT_TYPES
    )
    return {
        "ready": ready,
        "columns_exact": columns == TARGET_COLUMNS,
        "column_contracts_exact": column_contracts == TARGET_COLUMN_CONTRACTS,
        "table_constraints_valid": table_constraints_valid,
        "foreign_keys_exact": foreign_keys_exact,
        "index_set_exact": index_set_exact,
        "automatic_indexes_exact": automatic_indexes_exact,
        "trigger_set_exact": trigger_set_exact,
        "indexes": index_contracts,
        "triggers": trigger_contracts,
    }


def _is_order_state_table(name: str) -> bool:
    if name == GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE:
        return False
    return bool(
        "order" in name
        or name
        in {
            "gateway_commands",
            "gateway_command_events",
            "gateway_command_dedupe_keys",
            "live_sim_lifecycle_inbox",
            "live_sim_lifecycle_consumer_runs",
        }
    )


def _validate_paths(*, source: Path, clone: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"source database was not found: {source}")
    if source == clone:
        raise ValueError("clone database must differ from source database")
    if _sidecar_count(source):
        raise RuntimeError("schema 62 -> 63 preflight source must have no WAL/SHM/journal sidecars")
    if any(Path(f"{clone}{suffix}").exists() for suffix in ("", *SIDECAR_SUFFIXES)):
        raise FileExistsError(f"clone database artifacts already exist: {clone}")


def _sidecar_count(path: Path) -> int:
    return sum(Path(f"{path}{suffix}").exists() for suffix in SIDECAR_SUFFIXES)


def _write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    verdict = _mapping(report.get("verdict"))
    summary_path.write_text(
        "\n".join(
            (
                "# Exact SQLite 62→63 migration preflight",
                "",
                f"- status: `{verdict.get('status')}`",
                f"- source schema: `{verdict.get('source_schema_version')}`",
                f"- target schema: `{verdict.get('target_schema_version')}`",
                f"- source files unchanged: `{verdict.get('source_files_unchanged')}`",
                f"- order state preserved: `{verdict.get('order_state_preserved')}`",
                f"- target ledger empty: `{verdict.get('target_ledger_empty')}`",
                f"- failures: `{','.join(verdict.get('failures') or []) or 'none'}`",
                "",
            )
        ),
        encoding="utf-8",
    )
    return {"raw_json": raw_path, "summary_markdown": summary_path}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
