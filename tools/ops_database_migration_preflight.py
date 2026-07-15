from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from storage.sqlite import (  # noqa: E402
    SCHEMA_VERSION,
    initialize_database,
    initialize_database_for_offline_migration,
    migrate_schema_61_to_62,
)

PREFLIGHT_CONTRACT_VERSION = "exact-61-to-62-v2"
EXACT_SCHEMA_61_TO_62_MIGRATION_METHOD = "exact_storage_migrate_schema_61_to_62"

REQUIRED_TARGET_TABLES = (
    "gateway_order_broker_boundary_resolutions",
    "gateway_order_broker_boundaries",
    "market_scan_projection_routing_decisions",
    "market_scan_append_only_budget_state",
    "live_sim_lifecycle_inbox",
    "live_sim_lifecycle_consumer_runs",
    "live_sim_lifecycle_routing_decisions",
    "incremental_evaluation_dead_letters",
    "incremental_evaluation_dead_letter_dispositions",
    "pipeline_coherency_dispositions",
)
REQUIRED_TARGET_COLUMNS = {
    "gateway_order_broker_boundary_resolutions": frozenset(
        {
            "resolution_id",
            "request_id",
            "request_hash",
            "command_id",
            "sequence_no",
            "action",
            "resolution_type",
            "supersedes_resolution_id",
            "reason_code",
            "evidence_type",
            "evidence_ref",
            "evidence_sha256",
            "operator_id",
            "source_boundary_fingerprint",
            "source_boundary_updated_at",
            "boundary_snapshot_json",
            "created_at",
            "live_sim_only",
            "live_real_allowed",
            "routing_fence_active",
        }
    ),
    "gateway_order_broker_boundaries": frozenset(
        {
            "command_id",
            "state",
            "broker_order_no",
            "pre_ack_recorded_at",
            "broker_accepted_at",
            "chejan_confirmed_at",
            "unconfirmed_at",
            "updated_at",
        }
    ),
    "incremental_evaluation_dead_letter_dispositions": frozenset(
        {
            "disposition_id",
            "request_id",
            "request_hash",
            "dead_letter_id",
            "sequence_no",
            "action",
            "supersedes_disposition_id",
            "reason_code",
            "operator_id",
            "expected_dead_letter_fingerprint",
            "expected_candidate_version",
            "evidence_ref",
            "evidence_sha256",
            "evidence_json",
            "recovery_session_id",
            "batch_size",
            "fencing_token",
            "safety_snapshot_json",
            "created_at",
            "observe_only",
            "live_sim_allowed",
            "live_real_allowed",
            "auto_run_evaluation",
        }
    ),
    "pipeline_coherency_dispositions": frozenset(
        {
            "disposition_id",
            "request_id",
            "request_hash",
            "candidate_instance_id",
            "subject_key",
            "trade_date",
            "order_plan_id",
            "sequence_no",
            "action",
            "supersedes_disposition_id",
            "reason_code",
            "operator_id",
            "expected_pipeline_fingerprint",
            "expected_subject_version",
            "expected_source_fingerprint",
            "expected_candidate_fingerprint",
            "expected_downstream_fingerprint",
            "expected_boundary_fingerprint",
            "evidence_type",
            "evidence_ref",
            "evidence_sha256",
            "evidence_json",
            "safety_snapshot_json",
            "created_at",
            "observe_only",
            "live_sim_allowed",
            "live_real_allowed",
            "order_commands_allowed",
            "not_order_intent",
            "no_order_side_effects",
            "auto_run_evaluation",
        }
    ),
}
REQUIRED_TARGET_TABLE_SQL_TOKENS = {
    "gateway_order_broker_boundary_resolutions": (
        "CHECK (SEQUENCE_NO > 0)",
        "RESOLVE_BROKER_NOT_REACHED",
        "CHECK (RESOLUTION_TYPE = 'BROKER_NOT_REACHED')",
        "CHECK (LIVE_SIM_ONLY = 1)",
        "CHECK (LIVE_REAL_ALLOWED = 0)",
        "CHECK (ROUTING_FENCE_ACTIVE = 1)",
        "FOREIGN KEY (COMMAND_ID)",
        "FOREIGN KEY (SUPERSEDES_RESOLUTION_ID)",
    ),
    "incremental_evaluation_dead_letter_dispositions": (
        "CHECK (SEQUENCE_NO > 0)",
        "DISPOSE_OBSOLETE_CLOSED_CANDIDATE",
        "RESET_CANARY",
        "VERIFY_CANARY",
        "RESET_BATCH",
        "CHECK (OBSERVE_ONLY = 1)",
        "CHECK (LIVE_SIM_ALLOWED = 0)",
        "CHECK (LIVE_REAL_ALLOWED = 0)",
        "CHECK (AUTO_RUN_EVALUATION = 0)",
        "CHECK (ACTION != 'RESET_CANARY' OR BATCH_SIZE = 1)",
        "CHECK (ACTION != 'VERIFY_CANARY' OR BATCH_SIZE = 1)",
        "OR (BATCH_SIZE BETWEEN 2 AND 5)",
        "UNIQUE (DEAD_LETTER_ID, SEQUENCE_NO)",
        "FOREIGN KEY (DEAD_LETTER_ID)",
        "FOREIGN KEY (SUPERSEDES_DISPOSITION_ID)",
    ),
    "pipeline_coherency_dispositions": (
        "DISPOSE_EXPIRED_PLAN_READY",
        "DISPOSE_ORPHAN_PIPELINE_OBSERVATION",
        "DISPOSE_STALE_OTHER_DATE",
        "AND OBSERVE_ONLY = 1",
        "AND LIVE_SIM_ALLOWED = 0",
        "AND LIVE_REAL_ALLOWED = 0",
        "AND ORDER_COMMANDS_ALLOWED = 0",
        "AND NOT_ORDER_INTENT = 1",
        "AND NO_ORDER_SIDE_EFFECTS = 1",
        "AND AUTO_RUN_EVALUATION = 0",
        "UNIQUE (SUBJECT_KEY, SEQUENCE_NO)",
        "FOREIGN KEY (SUPERSEDES_DISPOSITION_ID)",
    ),
}
REQUIRED_TARGET_INDEX_CONTRACTS: dict[str, dict[str, Any]] = {
    "idx_gateway_order_boundary_resolutions_created": {
        "table": "gateway_order_broker_boundary_resolutions",
        "columns": ("created_at", "resolution_id"),
        "unique": False,
        "partial": False,
        "descending": (True, True),
    },
    "uq_gateway_order_boundary_resolutions_request_id": {
        "table": "gateway_order_broker_boundary_resolutions",
        "columns": ("request_id",),
        "unique": True,
        "partial": False,
        "descending": (False,),
    },
    "uq_gateway_order_boundary_resolutions_command_sequence": {
        "table": "gateway_order_broker_boundary_resolutions",
        "columns": ("command_id", "sequence_no"),
        "unique": True,
        "partial": False,
        "descending": (False, False),
    },
    "uq_gateway_order_boundary_idempotency": {
        "table": "gateway_order_broker_boundaries",
        "columns": ("idempotency_key",),
        "unique": True,
        "partial": True,
        "descending": (False,),
        "where_sql": "WHERE IDEMPOTENCY_KEY IS NOT NULL",
    },
    "idx_gateway_order_boundary_state_updated": {
        "table": "gateway_order_broker_boundaries",
        "columns": ("state", "updated_at"),
        "unique": False,
        "partial": False,
        "descending": (False, False),
    },
    "idx_incremental_evaluation_dead_letter_candidate_time": {
        "table": "incremental_evaluation_dead_letters",
        "columns": ("candidate_instance_id", "dead_lettered_at", "dead_letter_id"),
        "unique": False,
        "partial": False,
        "descending": (False, True, True),
    },
    "idx_incremental_evaluation_dead_letter_status_time": {
        "table": "incremental_evaluation_dead_letters",
        "columns": ("status", "dead_lettered_at"),
        "unique": False,
        "partial": False,
        "descending": (False, True),
    },
    "idx_incremental_dead_letter_disposition_effective": {
        "table": "incremental_evaluation_dead_letter_dispositions",
        "columns": ("dead_letter_id", "sequence_no"),
        "unique": False,
        "partial": False,
        "descending": (False, True),
    },
    "idx_incremental_dead_letter_disposition_session": {
        "table": "incremental_evaluation_dead_letter_dispositions",
        "columns": ("recovery_session_id", "created_at", "disposition_id"),
        "unique": False,
        "partial": True,
        "descending": (False, False, False),
        "where_sql": "WHERE RECOVERY_SESSION_ID IS NOT NULL",
    },
    "idx_pipeline_coherency_disposition_effective": {
        "table": "pipeline_coherency_dispositions",
        "columns": ("subject_key", "sequence_no"),
        "unique": False,
        "partial": False,
        "descending": (False, True),
    },
    "idx_pipeline_coherency_disposition_action_created": {
        "table": "pipeline_coherency_dispositions",
        "columns": ("action", "created_at"),
        "unique": False,
        "partial": False,
        "descending": (False, True),
    },
}
REQUIRED_TARGET_TRIGGER_CONTRACTS: dict[str, dict[str, str]] = {
    "trg_gateway_order_boundary_resolutions_no_update": {
        "table": "gateway_order_broker_boundary_resolutions",
        "operation": "UPDATE",
        "error_message": "gateway order-boundary resolutions are append-only",
    },
    "trg_gateway_order_boundary_resolutions_no_delete": {
        "table": "gateway_order_broker_boundary_resolutions",
        "operation": "DELETE",
        "error_message": "gateway order-boundary resolutions are append-only",
    },
    "trg_incremental_evaluation_dead_letters_no_update": {
        "table": "incremental_evaluation_dead_letters",
        "operation": "UPDATE",
        "error_message": "incremental evaluation dead letters are immutable",
    },
    "trg_incremental_evaluation_dead_letters_no_delete": {
        "table": "incremental_evaluation_dead_letters",
        "operation": "DELETE",
        "error_message": "incremental evaluation dead letters are immutable",
    },
    "trg_incremental_dead_letter_dispositions_no_update": {
        "table": "incremental_evaluation_dead_letter_dispositions",
        "operation": "UPDATE",
        "error_message": ("incremental evaluation dead-letter dispositions are append-only"),
    },
    "trg_incremental_dead_letter_dispositions_no_delete": {
        "table": "incremental_evaluation_dead_letter_dispositions",
        "operation": "DELETE",
        "error_message": ("incremental evaluation dead-letter dispositions are append-only"),
    },
    "trg_pipeline_coherency_dispositions_no_update": {
        "table": "pipeline_coherency_dispositions",
        "operation": "UPDATE",
        "error_message": "pipeline coherency dispositions are append-only",
    },
    "trg_pipeline_coherency_dispositions_no_delete": {
        "table": "pipeline_coherency_dispositions",
        "operation": "DELETE",
        "error_message": "pipeline coherency dispositions are append-only",
    },
}
FINGERPRINT_FILES = {
    "main": "",
    "wal": "-wal",
    "shm": "-shm",
    "journal": "-journal",
}
MIGRATION_MUTABLE_TABLES: frozenset[str] = frozenset()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Clone a SQLite operating database read-only and validate the current "
            "schema migration on the clone."
        )
    )
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--clone-db", required=True)
    parser.add_argument("--require-source-schema", required=True)
    parser.add_argument("--skip-quick-check", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "database_migration_preflight"),
    )
    args = parser.parse_args()

    report = run_preflight(
        source_db=Path(args.source_db),
        clone_db=Path(args.clone_db),
        required_source_schema=args.require_source_schema,
        run_quick_check=not args.skip_quick_check,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report), flush=True)
    return 0 if report["verdict"]["status"] == "PASS" else 2


def run_preflight(
    *,
    source_db: Path,
    clone_db: Path,
    required_source_schema: str | None,
    run_quick_check: bool,
    out_dir: Path,
) -> dict[str, Any]:
    source = source_db.expanduser().resolve()
    clone = clone_db.expanduser().resolve()
    _validate_paths(source=source, clone=clone)

    source_files_before = _file_fingerprints(source)
    disk_space = _disk_space_contract(source=source, clone=clone)
    if disk_space["sufficient"] is not True:
        raise RuntimeError("insufficient free space for migration preflight clone")
    source_connection = _open_read_only(source)
    try:
        source_quick_check_started = time.perf_counter()
        source_quick_check = [
            str(row[0]) for row in source_connection.execute("PRAGMA quick_check(1)")
        ]
        source_quick_check_elapsed_sec = time.perf_counter() - source_quick_check_started
        source_snapshot = _database_snapshot(source_connection)
        if (
            required_source_schema is not None
            and source_snapshot["schema_version"] != required_source_schema
        ):
            raise RuntimeError(
                "source schema mismatch: "
                f"expected={required_source_schema} "
                f"actual={source_snapshot['schema_version']}"
            )

        clone.parent.mkdir(parents=True, exist_ok=True)
        backup_started = time.perf_counter()
        destination = sqlite3.connect(clone, timeout=60.0)
        try:
            source_connection.backup(
                destination,
                pages=65_536,
                progress=_backup_progress_printer(),
                sleep=0.05,
            )
        finally:
            destination.close()
        backup_elapsed_sec = time.perf_counter() - backup_started
    finally:
        source_connection.close()

    source_files_after = _file_fingerprints(source)
    clone_before = _snapshot_path(clone)

    migration_started = time.perf_counter()
    migration_method = _migrate_clone_to_current_schema(
        clone,
        source_schema=str(source_snapshot["schema_version"]),
    )
    migration_elapsed_sec = time.perf_counter() - migration_started

    exact_quick_check_started = time.perf_counter()
    exact_quick_check = _quick_check(clone) if run_quick_check else ["SKIPPED"]
    exact_quick_check_elapsed_sec = time.perf_counter() - exact_quick_check_started
    clone_after_exact = _snapshot_path(clone)

    contract_probe_error_type: str | None = None
    try:
        contract_probes = _probe_target_contracts(clone)
    except sqlite3.Error as exc:
        contract_probes = {}
        contract_probe_error_type = type(exc).__name__
    clone_after_probes = _snapshot_path(clone)

    idempotent_started = time.perf_counter()
    idempotent = initialize_database(clone)
    idempotent.close()
    idempotent_elapsed_sec = time.perf_counter() - idempotent_started
    clone_after_idempotent = _snapshot_path(clone)

    quick_check_started = time.perf_counter()
    quick_check = _quick_check(clone) if run_quick_check else ["SKIPPED"]
    quick_check_elapsed_sec = time.perf_counter() - quick_check_started

    report: dict[str, Any] = {
        "generated_at": _now(),
        "preflight_contract_version": PREFLIGHT_CONTRACT_VERSION,
        "source": {
            "path": str(source),
            "files_before": source_files_before,
            "files_after": source_files_after,
            "snapshot": source_snapshot,
            "quick_check": source_quick_check,
            "quick_check_elapsed_sec": source_quick_check_elapsed_sec,
            "opened_read_only": True,
            "query_only": True,
            "immutable": True,
        },
        "clone": {
            "path": str(clone),
            "disk_space": disk_space,
            "backup_elapsed_sec": backup_elapsed_sec,
            "migration_method": migration_method,
            "migration_elapsed_sec": migration_elapsed_sec,
            "exact_quick_check_elapsed_sec": exact_quick_check_elapsed_sec,
            "idempotent_elapsed_sec": idempotent_elapsed_sec,
            "quick_check_elapsed_sec": quick_check_elapsed_sec,
            "before_migration": clone_before,
            "after_migration": clone_after_exact,
            "after_exact_migration": clone_after_exact,
            "after_contract_probes": clone_after_probes,
            "after_idempotent_rerun": clone_after_idempotent,
            "contract_probes": contract_probes,
            "contract_probe_error_type": contract_probe_error_type,
            "exact_quick_check": exact_quick_check,
            "quick_check": quick_check,
        },
        "target_schema_version": str(SCHEMA_VERSION),
        "required_source_schema": required_source_schema,
    }
    report["verdict"] = evaluate_report(report)
    report_paths = _write_report(report, out_dir=out_dir)
    report["report_paths"] = {name: str(path) for name, path in report_paths.items()}
    return report


def _migrate_clone_to_current_schema(path: Path, *, source_schema: str) -> str:
    if source_schema == "61" and SCHEMA_VERSION == 62:
        connection = sqlite3.connect(path, timeout=60.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            connection.execute("BEGIN EXCLUSIVE")
            migrate_schema_61_to_62(connection)
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
        return EXACT_SCHEMA_61_TO_62_MIGRATION_METHOD

    migrated = initialize_database_for_offline_migration(path)
    migrated.close()
    return "initialize_database_legacy_or_idempotent"


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(report.get("source"))
    clone = _mapping(report.get("clone"))
    source_snapshot = _mapping(source.get("snapshot"))
    clone_before = _mapping(clone.get("before_migration"))
    clone_after = _mapping(clone.get("after_migration"))
    clone_after_exact = _mapping(clone.get("after_exact_migration"))
    clone_after_probes = _mapping(clone.get("after_contract_probes"))
    clone_after_idempotent = _mapping(clone.get("after_idempotent_rerun"))
    failures: list[str] = []

    if report.get("preflight_contract_version") != PREFLIGHT_CONTRACT_VERSION:
        failures.append("PREFLIGHT_CONTRACT_VERSION_INVALID")

    exact_snapshot_alias_valid = bool(
        clone_after and clone_after_exact and clone_after == clone_after_exact
    )
    if not exact_snapshot_alias_valid:
        failures.append("EXACT_SNAPSHOT_ALIAS_MISMATCH")

    probe_rollback_no_change = bool(
        clone_after_exact and clone_after_probes and clone_after_probes == clone_after_exact
    )
    if not probe_rollback_no_change:
        failures.append("CONTRACT_PROBES_CHANGED_DATABASE")

    idempotent_schema_unchanged = bool(
        clone_after_probes
        and clone_after_idempotent
        and all(
            clone_after_idempotent.get(key) == clone_after_probes.get(key)
            for key in (
                "schema_version",
                "journal_mode",
                "table_count",
                "schema_objects",
                "required_tables",
                "required_columns",
                "required_indexes",
                "required_triggers",
            )
        )
    )
    idempotent_table_content_unchanged = bool(
        clone_after_probes
        and clone_after_idempotent
        and clone_after_idempotent.get("table_content") == clone_after_probes.get("table_content")
        and clone_after_idempotent.get("projection_outbox")
        == clone_after_probes.get("projection_outbox")
        and clone_after_idempotent.get("sqlite_sequence")
        == clone_after_probes.get("sqlite_sequence")
    )
    idempotent_rerun_no_change = bool(
        clone_after_probes
        and clone_after_idempotent
        and clone_after_idempotent == clone_after_probes
    )
    if not idempotent_rerun_no_change:
        failures.append("IDEMPOTENT_RERUN_CHANGED_DATABASE")

    required_source_schema = report.get("required_source_schema")
    if not required_source_schema:
        failures.append("SOURCE_SCHEMA_REQUIREMENT_MISSING")
    elif source_snapshot.get("schema_version") != str(required_source_schema):
        failures.append("SOURCE_SCHEMA_MISMATCH")
    try:
        source_schema_number = int(source_snapshot.get("schema_version") or -1)
        target_schema_number = int(report.get("target_schema_version") or -1)
    except (TypeError, ValueError):
        source_schema_number = -1
        target_schema_number = -1
    if source_schema_number < 0:
        failures.append("SOURCE_SCHEMA_INVALID")
    if target_schema_number < 0:
        failures.append("TARGET_SCHEMA_INVALID")
    if source_schema_number > target_schema_number >= 0:
        failures.append("SOURCE_SCHEMA_NEWER_THAN_TARGET")
    elif source_schema_number == target_schema_number >= 0:
        failures.append("SOURCE_SCHEMA_NOT_OLDER_THAN_TARGET")
    if clone_before.get("schema_version") != source_snapshot.get("schema_version"):
        failures.append("BACKUP_SCHEMA_MISMATCH")
    backup_schema_objects_preserved = bool(
        clone_before.get("schema_objects")
        and clone_before.get("schema_objects") == source_snapshot.get("schema_objects")
    )
    if not backup_schema_objects_preserved:
        failures.append("BACKUP_SCHEMA_OBJECT_MISMATCH")
    backup_schema_version_record_preserved = bool(
        clone_before.get("schema_version_record")
        and clone_before.get("schema_version_record")
        == source_snapshot.get("schema_version_record")
    )
    if not backup_schema_version_record_preserved:
        failures.append("BACKUP_SCHEMA_VERSION_RECORD_MISMATCH")
    if clone_after.get("schema_version") != str(report.get("target_schema_version")):
        failures.append("TARGET_SCHEMA_MISMATCH")
    if clone_before.get("projection_outbox") != source_snapshot.get("projection_outbox"):
        failures.append("BACKUP_OUTBOX_MISMATCH")
    if clone_after.get("projection_outbox") != source_snapshot.get("projection_outbox"):
        failures.append("MIGRATION_OUTBOX_MISMATCH")
    if clone_before.get("sqlite_sequence") != source_snapshot.get("sqlite_sequence"):
        failures.append("BACKUP_SQLITE_SEQUENCE_MISMATCH")
    if clone_after.get("sqlite_sequence") != source_snapshot.get("sqlite_sequence"):
        failures.append("MIGRATION_SQLITE_SEQUENCE_MISMATCH")
    backup_table_changes = _changed_table_content(
        before=_mapping(source_snapshot.get("table_content")),
        after=_mapping(clone_before.get("table_content")),
    )
    if backup_table_changes:
        failures.append("BACKUP_TABLE_CONTENT_MISMATCH")
    migration_table_changes = _changed_table_content(
        before=_mapping(source_snapshot.get("table_content")),
        after=_mapping(clone_after.get("table_content")),
        excluded=MIGRATION_MUTABLE_TABLES,
    )
    if migration_table_changes:
        failures.append("MIGRATION_TABLE_CONTENT_MISMATCH")
    exact_migration_table_changes = _changed_table_content(
        before=_mapping(source_snapshot.get("table_content")),
        after=_mapping(clone_after_exact.get("table_content")),
        excluded=MIGRATION_MUTABLE_TABLES,
    )
    contract_probes = _mapping(clone.get("contract_probes"))
    if not contract_probes or not all(value is True for value in contract_probes.values()):
        failures.append("TARGET_BEHAVIOR_CONTRACT_INVALID")
    disk_space = _mapping(clone.get("disk_space"))
    if (
        not disk_space
        or disk_space.get("sufficient") is not True
        or not isinstance(disk_space.get("free_bytes"), int)
        or not isinstance(disk_space.get("required_bytes"), int)
    ):
        failures.append("CLONE_DISK_SPACE_INSUFFICIENT")
    source_table_content = _mapping(source_snapshot.get("table_content"))
    clone_table_content = _mapping(clone_after.get("table_content"))
    resolution_was_absent = "gateway_order_broker_boundary_resolutions" not in source_table_content
    pretarget_resolution_table_present = bool(
        source_schema_number < 60 and not resolution_was_absent
    )
    if pretarget_resolution_table_present:
        failures.append("SOURCE_PRETARGET_RESOLUTION_TABLE_PRESENT")
    target_resolution = _mapping(
        clone_table_content.get("gateway_order_broker_boundary_resolutions")
    )
    target_resolution_ledger_empty = bool(
        not pretarget_resolution_table_present
        and (not resolution_was_absent or int(target_resolution.get("row_count") or 0) == 0)
    )
    if not target_resolution_ledger_empty:
        failures.append("TARGET_RESOLUTION_LEDGER_NOT_EMPTY")

    disposition_was_absent = (
        "incremental_evaluation_dead_letter_dispositions" not in source_table_content
    )
    pretarget_disposition_table_present = bool(
        source_schema_number < 61 and not disposition_was_absent
    )
    if pretarget_disposition_table_present:
        failures.append("SOURCE_PRETARGET_DEAD_LETTER_DISPOSITION_TABLE_PRESENT")
    target_disposition = _mapping(
        clone_table_content.get("incremental_evaluation_dead_letter_dispositions")
    )
    target_disposition_ledger_empty = bool(
        not pretarget_disposition_table_present
        and (not disposition_was_absent or int(target_disposition.get("row_count") or 0) == 0)
    )
    if not target_disposition_ledger_empty:
        failures.append("TARGET_DEAD_LETTER_DISPOSITION_LEDGER_NOT_EMPTY")

    pipeline_disposition_was_absent = "pipeline_coherency_dispositions" not in source_table_content
    pretarget_pipeline_disposition_table_present = bool(
        source_schema_number < 62 and not pipeline_disposition_was_absent
    )
    if pretarget_pipeline_disposition_table_present:
        failures.append("SOURCE_PRETARGET_PIPELINE_DISPOSITION_TABLE_PRESENT")
    target_pipeline_disposition = _mapping(
        clone_table_content.get("pipeline_coherency_dispositions")
    )
    target_pipeline_disposition_ledger_empty = bool(
        not pretarget_pipeline_disposition_table_present
        and (
            not pipeline_disposition_was_absent
            or int(target_pipeline_disposition.get("row_count") or 0) == 0
        )
    )
    if not target_pipeline_disposition_ledger_empty:
        failures.append("TARGET_PIPELINE_DISPOSITION_LEDGER_NOT_EMPTY")

    exact_migration_method = clone.get("migration_method") == EXACT_SCHEMA_61_TO_62_MIGRATION_METHOD
    source_table_names = set(source_table_content)
    exact_table_names = set(_mapping(clone_after_exact.get("table_content")))
    expected_exact_table_names = source_table_names | {"pipeline_coherency_dispositions"}
    exact_target_table_set_valid: bool | None = None
    if exact_migration_method:
        exact_target_table_set_valid = exact_table_names == expected_exact_table_names
        if not exact_target_table_set_valid:
            failures.append("EXACT_MIGRATION_TABLE_SET_INVALID")

    required_tables = _mapping(clone_after.get("required_tables"))
    if not all(bool(required_tables.get(table)) for table in REQUIRED_TARGET_TABLES):
        failures.append("TARGET_TABLE_MISSING")
    required_columns = _mapping(clone_after.get("required_columns"))
    if not all(
        bool(_mapping(required_columns.get(table)).get("valid"))
        for table in REQUIRED_TARGET_COLUMNS
    ):
        failures.append("TARGET_COLUMN_CONTRACT_INVALID")
    required_indexes = _mapping(clone_after.get("required_indexes"))
    if not all(
        bool(_mapping(required_indexes.get(index_name)).get("valid"))
        for index_name in REQUIRED_TARGET_INDEX_CONTRACTS
    ):
        failures.append("TARGET_INDEX_CONTRACT_INVALID")
    required_triggers = _mapping(clone_after.get("required_triggers"))
    if not all(
        bool(_mapping(required_triggers.get(trigger_name)).get("valid"))
        for trigger_name in REQUIRED_TARGET_TRIGGER_CONTRACTS
    ):
        failures.append("TARGET_APPEND_ONLY_TRIGGER_CONTRACT_INVALID")
    if clone.get("exact_quick_check") == ["SKIPPED"]:
        failures.append("EXACT_CLONE_QUICK_CHECK_SKIPPED")
    elif clone.get("exact_quick_check") != ["ok"]:
        failures.append("EXACT_CLONE_QUICK_CHECK_FAILED")
    if clone.get("quick_check") == ["SKIPPED"]:
        failures.append("CLONE_QUICK_CHECK_SKIPPED")
    elif clone.get("quick_check") != ["ok"]:
        failures.append("CLONE_QUICK_CHECK_FAILED")
    if source.get("quick_check") != ["ok"]:
        failures.append("SOURCE_QUICK_CHECK_FAILED")

    before_files = _mapping(source.get("files_before"))
    after_files = _mapping(source.get("files_after"))
    changed_source_files = _changed_source_data_files(
        before=before_files,
        after=after_files,
    )
    if changed_source_files:
        failures.append("SOURCE_DATA_FILE_CHANGED")

    required_tables_present = all(
        bool(required_tables.get(table)) for table in REQUIRED_TARGET_TABLES
    )
    required_columns_present = all(
        bool(_mapping(required_columns.get(table)).get("valid"))
        for table in REQUIRED_TARGET_COLUMNS
    )
    required_indexes_present = all(
        bool(_mapping(required_indexes.get(index_name)).get("valid"))
        for index_name in REQUIRED_TARGET_INDEX_CONTRACTS
    )
    required_append_only_triggers_present = all(
        bool(_mapping(required_triggers.get(trigger_name)).get("valid"))
        for trigger_name in REQUIRED_TARGET_TRIGGER_CONTRACTS
    )
    target_behavior_contract_valid = bool(
        contract_probes and all(value is True for value in contract_probes.values())
    )
    exact_migration_target_valid: bool | None = None
    if exact_migration_method:
        exact_migration_target_valid = bool(
            clone_after_exact.get("schema_version") == str(report.get("target_schema_version"))
            and exact_target_table_set_valid
            and not exact_migration_table_changes
            and clone_after_exact.get("projection_outbox")
            == source_snapshot.get("projection_outbox")
            and clone_after_exact.get("sqlite_sequence") == source_snapshot.get("sqlite_sequence")
            and target_pipeline_disposition_ledger_empty
            and required_tables_present
            and required_columns_present
            and required_indexes_present
            and required_append_only_triggers_present
            and clone.get("exact_quick_check") == ["ok"]
            and target_behavior_contract_valid
        )
        if not exact_migration_target_valid:
            failures.append("EXACT_MIGRATION_TARGET_INVALID")

    return {
        "status": "FAIL" if failures else "PASS",
        "failures": failures,
        "source_schema_version": source_snapshot.get("schema_version"),
        "target_schema_version": clone_after.get("schema_version"),
        "source_data_files_unchanged": not changed_source_files,
        "changed_source_files": changed_source_files,
        "outbox_preserved": (
            clone_after.get("projection_outbox") == source_snapshot.get("projection_outbox")
        ),
        "sqlite_sequence_preserved": bool(
            clone_before.get("sqlite_sequence")
            == source_snapshot.get("sqlite_sequence")
            == clone_after.get("sqlite_sequence")
        ),
        "backup_table_content_preserved": not backup_table_changes,
        "migration_table_content_preserved": not migration_table_changes,
        "backup_schema_objects_preserved": backup_schema_objects_preserved,
        "backup_schema_version_record_preserved": (backup_schema_version_record_preserved),
        "backup_table_content_changes": backup_table_changes,
        "migration_table_content_changes": migration_table_changes,
        "exact_migration_table_content_changes": exact_migration_table_changes,
        "exact_snapshot_alias_valid": exact_snapshot_alias_valid,
        "exact_target_table_set_valid": exact_target_table_set_valid,
        "exact_migration_target_valid": exact_migration_target_valid,
        "probe_rollback_no_change": probe_rollback_no_change,
        "idempotent_rerun_no_change": idempotent_rerun_no_change,
        "idempotent_schema_unchanged": idempotent_schema_unchanged,
        "idempotent_table_content_unchanged": idempotent_table_content_unchanged,
        "target_behavior_contract_valid": target_behavior_contract_valid,
        "clone_disk_space_sufficient": disk_space.get("sufficient") is True,
        "target_resolution_ledger_empty": target_resolution_ledger_empty,
        "target_dead_letter_disposition_ledger_empty": (target_disposition_ledger_empty),
        "target_pipeline_disposition_ledger_empty": (target_pipeline_disposition_ledger_empty),
        "required_tables_present": required_tables_present,
        "required_columns_present": required_columns_present,
        "required_indexes_present": required_indexes_present,
        "required_append_only_triggers_present": required_append_only_triggers_present,
        "exact_quick_check": clone.get("exact_quick_check"),
        "quick_check": clone.get("quick_check"),
        "operating_database_mutated": bool(changed_source_files),
    }


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    clone = _mapping(report.get("clone"))
    return (
        f"database migration preflight: {verdict.get('status', 'UNKNOWN')} "
        f"schema={verdict.get('source_schema_version')}->"
        f"{verdict.get('target_schema_version')} "
        f"backup={float(clone.get('backup_elapsed_sec') or 0):.3f}s "
        f"migration={float(clone.get('migration_elapsed_sec') or 0):.3f}s "
        f"quick_check={float(clone.get('quick_check_elapsed_sec') or 0):.3f}s"
    )


def _validate_paths(*, source: Path, clone: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"source database was not found: {source}")
    if source == clone:
        raise ValueError("clone database must differ from source database")
    source_sidecars = tuple(Path(f"{source}{suffix}") for suffix in ("-wal", "-shm", "-journal"))
    if any(sidecar.exists() for sidecar in source_sidecars):
        raise RuntimeError(
            "source database must be quiescent with no WAL/SHM/rollback-journal sidecars"
        )
    clone_artifacts = tuple(Path(f"{clone}{suffix}") for suffix in ("", "-wal", "-shm", "-journal"))
    if any(artifact.exists() for artifact in clone_artifacts):
        raise FileExistsError(f"clone database artifacts already exist: {clone}")


def _disk_space_contract(*, source: Path, clone: Path) -> dict[str, Any]:
    source_bytes = sum(
        candidate.stat().st_size
        for candidate in (
            source,
            Path(f"{source}-wal"),
            Path(f"{source}-shm"),
            Path(f"{source}-journal"),
        )
        if candidate.exists()
    )
    probe = clone.parent
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    free_bytes = int(shutil.disk_usage(probe).free)
    required_bytes = max(
        int(source_bytes) * 3,
        int(source_bytes) + 256 * 1024 * 1024,
    )
    return {
        "free_bytes": free_bytes,
        "required_bytes": required_bytes,
        "source_bytes": int(source_bytes),
        "sufficient": free_bytes >= required_bytes,
    }


def _open_read_only(path: Path) -> sqlite3.Connection:
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=ro&immutable=1",
        uri=True,
        timeout=60.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _snapshot_path(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(path, timeout=60.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        return _database_snapshot(connection)
    finally:
        connection.close()


def _database_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = {
        str(row["name"])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    schema_version = None
    if "app_metadata" in tables:
        row = connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()
        schema_version = None if row is None else str(row["value"])
    outbox: dict[str, int] = {}
    if "projection_outbox" in tables:
        outbox = {
            str(row["status"]): int(row["row_count"])
            for row in connection.execute(
                "SELECT status, COUNT(*) AS row_count "
                "FROM projection_outbox GROUP BY status ORDER BY status"
            )
        }
    sqlite_sequence: dict[str, int] = {}
    if "sqlite_sequence" in tables:
        sqlite_sequence = {
            str(row["name"]): int(row["seq"] or 0)
            for row in connection.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        }
    return {
        "schema_version": schema_version,
        "schema_version_record": _schema_version_record_fingerprint(
            connection,
            tables=tables,
        ),
        "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]),
        "table_count": len(tables),
        "schema_objects": _schema_object_fingerprint(connection),
        "projection_outbox": outbox,
        "sqlite_sequence": sqlite_sequence,
        "table_content": _table_content_fingerprints(
            connection,
            tables={table for table in tables if not table.startswith("sqlite_")},
        ),
        "required_tables": {table: table in tables for table in REQUIRED_TARGET_TABLES},
        "required_columns": _required_column_contracts(connection, tables=tables),
        "required_indexes": _required_index_contracts(connection),
        "required_triggers": _required_trigger_contracts(connection),
    }


def _schema_version_record_fingerprint(
    connection: sqlite3.Connection,
    *,
    tables: set[str],
) -> dict[str, Any]:
    """Hash the schema-version row while normalizing only its value.

    Source and target legitimately contain different schema-version values. The
    row identity, SQL storage type, updated_at, and any future columns must still
    remain observable for exact-target no-op and post-commit comparisons.
    """

    digest = hashlib.sha256()
    row_count = 0
    if "app_metadata" not in tables:
        return {"row_count": row_count, "sha256": digest.hexdigest()}

    column_rows = connection.execute("PRAGMA table_info(app_metadata)").fetchall()
    columns = [str(row["name"]) for row in column_rows]
    digest.update(json.dumps(columns, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    if "key" not in columns or "value" not in columns:
        return {"row_count": row_count, "sha256": digest.hexdigest()}

    table_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'app_metadata'"
    ).fetchone()
    table_sql = "" if table_row is None else str(table_row["sql"] or "")
    include_rowid = "WITHOUT ROWID" not in table_sql.upper()
    column_sql = ", ".join(_quote_identifier(column) for column in columns)
    select_sql = (
        f"_rowid_ AS {_quote_identifier('__codex_rowid__')}, {column_sql}"
        if include_rowid
        else column_sql
    )
    for row in connection.execute(
        f"SELECT {select_sql} FROM app_metadata WHERE key = ?",
        ("schema_version",),
    ):
        values: list[dict[str, Any]] = []
        for column in columns:
            typed_value = _sql_value_for_hash(row[column])
            if column == "value":
                typed_value = {**typed_value, "value": "<normalized-schema-version>"}
            values.append(typed_value)
        if include_rowid:
            values.insert(
                0,
                {
                    "type": "rowid",
                    "value": int(row["__codex_rowid__"]),
                },
            )
        encoded = json.dumps(
            values,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        row_count += 1
    return {"row_count": row_count, "sha256": digest.hexdigest()}


def _schema_object_fingerprint(connection: sqlite3.Connection) -> dict[str, Any]:
    digest = hashlib.sha256()
    object_count = 0
    rows = connection.execute(
        """
        SELECT type, name, tbl_name, COALESCE(sql, '') AS sql
        FROM sqlite_master
        WHERE type IN ('table', 'index', 'trigger', 'view')
        ORDER BY type, name, tbl_name
        """
    )
    for row in rows:
        encoded = json.dumps(
            [
                str(row["type"]),
                str(row["name"]),
                str(row["tbl_name"]),
                str(row["sql"]),
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        object_count += 1
    return {"object_count": object_count, "sha256": digest.hexdigest()}


def _required_column_contracts(
    connection: sqlite3.Connection,
    *,
    tables: set[str],
) -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for table_name, required in REQUIRED_TARGET_COLUMNS.items():
        columns = (
            {
                str(row["name"])
                for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table_name)})")
            }
            if table_name in tables
            else set()
        )
        missing = sorted(required - columns)
        table_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        table_sql = "" if table_row is None else str(table_row["sql"] or "")
        normalized_table_sql = " ".join(table_sql.upper().split())
        required_tokens = REQUIRED_TARGET_TABLE_SQL_TOKENS.get(table_name, ())
        missing_sql_tokens = [
            token
            for token in required_tokens
            if " ".join(token.upper().split()) not in normalized_table_sql
        ]
        contracts[table_name] = {
            "exists": table_name in tables,
            "required_columns": sorted(required),
            "missing_columns": missing,
            "missing_sql_tokens": missing_sql_tokens,
            "valid": table_name in tables and not missing and not missing_sql_tokens,
        }
    return contracts


def _required_index_contracts(
    connection: sqlite3.Connection,
) -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for index_name, expected in REQUIRED_TARGET_INDEX_CONTRACTS.items():
        table_name = str(expected["table"])
        index_rows = connection.execute(
            f"PRAGMA index_list({_quote_identifier(table_name)})"
        ).fetchall()
        index_row = next(
            (row for row in index_rows if str(row["name"]) == index_name),
            None,
        )
        index_xinfo = connection.execute(
            f"PRAGMA index_xinfo({_quote_identifier(index_name)})"
        ).fetchall()
        key_rows = [row for row in index_xinfo if int(row["key"] or 0) == 1]
        columns = tuple(str(row["name"]) for row in key_rows)
        descending = tuple(bool(row["desc"]) for row in key_rows)
        exists = index_row is not None
        unique = bool(index_row["unique"]) if index_row is not None else False
        partial = bool(index_row["partial"]) if index_row is not None else False
        expected_columns = tuple(str(item) for item in expected["columns"])
        expected_descending = tuple(bool(item) for item in expected.get("descending", ()))
        sql_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            (index_name,),
        ).fetchone()
        index_sql = "" if sql_row is None else str(sql_row["sql"] or "")
        normalized_index_sql = " ".join(index_sql.upper().split())
        where_sql = str(expected.get("where_sql") or "")
        where_valid = not where_sql or (" ".join(where_sql.upper().split()) in normalized_index_sql)
        contracts[index_name] = {
            "exists": exists,
            "table": table_name,
            "columns": list(columns),
            "expected_columns": list(expected_columns),
            "descending": list(descending),
            "expected_descending": list(expected_descending),
            "unique": unique,
            "expected_unique": bool(expected["unique"]),
            "partial": partial,
            "expected_partial": bool(expected["partial"]),
            "where_valid": where_valid,
            "valid": (
                exists
                and columns == expected_columns
                and descending == expected_descending
                and unique is bool(expected["unique"])
                and partial is bool(expected["partial"])
                and where_valid
            ),
        }
    return contracts


def _required_trigger_contracts(
    connection: sqlite3.Connection,
) -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for trigger_name, expected in REQUIRED_TARGET_TRIGGER_CONTRACTS.items():
        row = connection.execute(
            "SELECT tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            (trigger_name,),
        ).fetchone()
        sql = "" if row is None else str(row["sql"] or "")
        table_name = str(expected["table"])
        operation = str(expected["operation"])
        error_message = str(expected["error_message"])
        compact_sql = "".join(sql.upper().split()).replace("IFNOTEXISTS", "").rstrip(";")
        expected_sql = (
            f"CREATETRIGGER{trigger_name.upper()}BEFORE{operation}ON"
            f"{table_name.upper()}BEGINSELECTRAISE(ABORT,"
            f"'{''.join(error_message.upper().split())}');END"
        )
        contracts[trigger_name] = {
            "exists": row is not None,
            "table": None if row is None else str(row["tbl_name"]),
            "expected_table": table_name,
            "operation": operation,
            "valid": bool(
                row is not None
                and str(row["tbl_name"]) == table_name
                and compact_sql == expected_sql
            ),
        }
    return contracts


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _probe_target_contracts(path: Path) -> dict[str, bool]:
    connection = sqlite3.connect(path, timeout=60.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    results: dict[str, bool] = {}
    connection.execute("SAVEPOINT target_contract_probe")
    try:
        boundary_values = (
            "probe-boundary-one",
            "probe-idempotency",
            "2026-01-01T00:00:00Z",
        )
        connection.execute(
            """
            INSERT INTO gateway_order_broker_boundaries (
                command_id, idempotency_key, command_type, source, state,
                created_at, updated_at, live_sim_only, live_real_allowed
            )
            VALUES (?, ?, 'send_order', 'migration_probe', 'UNCONFIRMED',
                    ?, ?, 1, 0)
            """,
            (*boundary_values, boundary_values[2]),
        )
        results["boundary_idempotency_unique"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundaries (
                command_id, idempotency_key, command_type, source, state,
                created_at, updated_at, live_sim_only, live_real_allowed
            )
            VALUES ('probe-boundary-two', 'probe-idempotency', 'send_order',
                    'migration_probe', 'UNCONFIRMED',
                    '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, 0)
            """,
        )
        resolution_values = (
            "probe-resolution-one",
            "probe-request-one",
            "0" * 64,
            "probe-boundary-one",
            1,
            "RESOLVE_BROKER_NOT_REACHED",
            "BROKER_NOT_REACHED",
            "PROBE_REASON",
            "PROBE_EVIDENCE",
            "probe-evidence",
            "1" * 64,
            "probe.operator",
            "2" * 64,
            "2026-01-01T00:00:00Z",
            "{}",
            "2026-01-01T00:00:00Z",
        )
        connection.execute(
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at, live_sim_only,
                live_real_allowed, routing_fence_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 1)
            """,
            resolution_values,
        )
        results["resolution_revoke_insert_supported"] = _expect_success(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type,
                supersedes_resolution_id, reason_code, evidence_type,
                evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at, live_sim_only,
                live_real_allowed, routing_fence_active
            )
            VALUES ('probe-resolution-valid-revoke', 'probe-request-valid-revoke',
                    ?, 'probe-boundary-one', 2, 'REVOKE',
                    'BROKER_NOT_REACHED', 'probe-resolution-one',
                    'PROBE_REVOKE_REASON', 'PROBE_REVOKE_EVIDENCE',
                    'probe-revoke-evidence', ?, 'probe.operator', ?,
                    '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:01Z', 1, 0, 1)
            """,
            ("f" * 64, "e" * 64, "d" * 64),
        )
        results["resolution_request_unique"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at
            )
            VALUES ('probe-resolution-request-duplicate', 'probe-request-one',
                    ?, 'probe-boundary-one', 3, 'REVOKE', 'BROKER_NOT_REACHED',
                    'PROBE_REASON', 'PROBE_EVIDENCE', 'probe-evidence-two', ?,
                    'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z')
            """,
            ("3" * 64, "4" * 64, "5" * 64),
        )
        results["resolution_sequence_unique"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at
            )
            VALUES ('probe-resolution-sequence-duplicate', 'probe-request-two',
                    ?, 'probe-boundary-one', 1, 'REVOKE', 'BROKER_NOT_REACHED',
                    'PROBE_REASON', 'PROBE_EVIDENCE', 'probe-evidence-three', ?,
                    'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z')
            """,
            ("6" * 64, "7" * 64, "8" * 64),
        )
        results["resolution_action_check"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at
            )
            VALUES ('probe-resolution-action-invalid', 'probe-request-three', ?,
                    'probe-boundary-one', 4, 'INVALID', 'BROKER_NOT_REACHED',
                    'PROBE_REASON', 'PROBE_EVIDENCE', 'probe-evidence-four', ?,
                    'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z')
            """,
            ("9" * 64, "a" * 64, "b" * 64),
        )
        results["resolution_sequence_positive_check"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at
            )
            VALUES ('probe-resolution-sequence-zero', 'probe-request-sequence-zero',
                    ?, 'probe-boundary-one', 0, 'REVOKE', 'BROKER_NOT_REACHED',
                    'PROBE_REASON', 'PROBE_EVIDENCE', 'probe-evidence-sequence-zero',
                    ?, 'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z')
            """,
            ("1" * 64, "2" * 64, "3" * 64),
        )
        results["resolution_type_check"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at
            )
            VALUES ('probe-resolution-type-invalid', 'probe-request-type-invalid',
                    ?, 'probe-boundary-one', 6, 'REVOKE', 'INVALID',
                    'PROBE_REASON', 'PROBE_EVIDENCE', 'probe-evidence-type-invalid',
                    ?, 'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z')
            """,
            ("4" * 64, "5" * 64, "6" * 64),
        )
        results["resolution_scope_check"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at, live_sim_only
            )
            VALUES ('probe-resolution-scope-invalid', 'probe-request-four', ?,
                    'probe-boundary-one', 5, 'REVOKE', 'BROKER_NOT_REACHED',
                    'PROBE_REASON', 'PROBE_EVIDENCE', 'probe-evidence-five', ?,
                    'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z', 0)
            """,
            ("c" * 64, "d" * 64, "e" * 64),
        )
        results["resolution_live_real_scope_check"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at, live_real_allowed
            )
            VALUES ('probe-resolution-live-real-invalid',
                    'probe-request-live-real-invalid', ?, 'probe-boundary-one',
                    7, 'REVOKE', 'BROKER_NOT_REACHED', 'PROBE_REASON',
                    'PROBE_EVIDENCE', 'probe-evidence-live-real-invalid', ?,
                    'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z', 1)
            """,
            ("7" * 64, "8" * 64, "9" * 64),
        )
        results["resolution_routing_fence_check"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at, routing_fence_active
            )
            VALUES ('probe-resolution-fence-invalid',
                    'probe-request-fence-invalid', ?, 'probe-boundary-one',
                    8, 'REVOKE', 'BROKER_NOT_REACHED', 'PROBE_REASON',
                    'PROBE_EVIDENCE', 'probe-evidence-fence-invalid', ?,
                    'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z', 0)
            """,
            ("a" * 64, "b" * 64, "c" * 64),
        )
        results["resolution_command_foreign_key"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type, reason_code,
                evidence_type, evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at
            )
            VALUES ('probe-resolution-command-orphan',
                    'probe-request-command-orphan', ?, 'probe-boundary-missing',
                    1, 'REVOKE', 'BROKER_NOT_REACHED', 'PROBE_REASON',
                    'PROBE_EVIDENCE', 'probe-evidence-command-orphan', ?,
                    'probe.operator', ?, '2026-01-01T00:00:00Z', '{}',
                    '2026-01-01T00:00:00Z')
            """,
            ("d" * 64, "e" * 64, "f" * 64),
        )
        results["resolution_supersedes_foreign_key"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO gateway_order_broker_boundary_resolutions (
                resolution_id, request_id, request_hash, command_id,
                sequence_no, action, resolution_type,
                supersedes_resolution_id, reason_code, evidence_type,
                evidence_ref, evidence_sha256, operator_id,
                source_boundary_fingerprint, source_boundary_updated_at,
                boundary_snapshot_json, created_at
            )
            VALUES ('probe-resolution-supersedes-orphan',
                    'probe-request-supersedes-orphan', ?, 'probe-boundary-one',
                    9, 'REVOKE', 'BROKER_NOT_REACHED', 'missing-resolution',
                    'PROBE_REASON', 'PROBE_EVIDENCE',
                    'probe-evidence-supersedes-orphan', ?, 'probe.operator', ?,
                    '2026-01-01T00:00:00Z', '{}', '2026-01-01T00:00:00Z')
            """,
            ("0" * 64, "1" * 64, "2" * 64),
        )
        results["resolution_update_blocked"] = _expect_integrity_error(
            connection,
            """
            UPDATE gateway_order_broker_boundary_resolutions
            SET reason_code = 'MUTATED'
            WHERE resolution_id = 'probe-resolution-one'
            """,
        )
        results["resolution_delete_blocked"] = _expect_integrity_error(
            connection,
            """
            DELETE FROM gateway_order_broker_boundary_resolutions
            WHERE resolution_id = 'probe-resolution-one'
            """,
        )
        connection.execute(
            """
            INSERT INTO incremental_evaluation_dead_letters (
                dead_letter_id, candidate_instance_id, trade_date, code,
                reason, source_event_id, priority, original_enqueued_at,
                last_queue_updated_at, attempts, last_error, status,
                dead_lettered_at
            )
            VALUES (
                'probe-dead-letter-one', 'probe-candidate-one', '2026-01-01',
                '005930', 'PRICE_TICK', 'probe-source-event', 0,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 3,
                'LEGACY_RETRY_EXHAUSTED', 'DEAD_LETTER',
                '2026-01-01T00:00:01Z'
            )
            """
        )
        results["incremental_dead_letter_multiple_generations_supported"] = _expect_success(
            connection,
            """
                INSERT INTO incremental_evaluation_dead_letters (
                    dead_letter_id, candidate_instance_id, trade_date, code,
                    reason, source_event_id, priority, original_enqueued_at,
                    last_queue_updated_at, attempts, last_error, status,
                    dead_lettered_at
                ) VALUES (
                    'probe-dead-letter-two', 'probe-candidate-one',
                    '2026-01-01', '005930', 'PRICE_TICK',
                    'probe-source-event-two', 0,
                    '2026-01-01T00:00:02Z', '2026-01-01T00:00:02Z', 3,
                    'SECOND_GENERATION', 'DEAD_LETTER',
                    '2026-01-01T00:00:03Z'
                )
                """,
        )
        connection.execute(
            """
            INSERT INTO incremental_evaluation_dead_letter_dispositions (
                disposition_id, request_id, request_hash, dead_letter_id,
                sequence_no, action, reason_code, operator_id,
                expected_dead_letter_fingerprint, expected_candidate_version,
                evidence_ref, evidence_sha256, evidence_json, created_at
            )
            VALUES (
                'probe-disposition-one', 'probe-disposition-request-one', ?,
                'probe-dead-letter-one', 1,
                'DISPOSE_OBSOLETE_CLOSED_CANDIDATE', 'PROBE_OBSOLETE',
                'probe.operator', ?, ?, 'probe-evidence', ?, '{}',
                '2026-01-01T00:00:02Z'
            )
            """,
            ("1" * 64, "2" * 64, "3" * 64, "4" * 64),
        )
        results["dead_letter_disposition_revoke_insert_supported"] = _expect_success(
            connection,
            """
            INSERT INTO incremental_evaluation_dead_letter_dispositions (
                disposition_id, request_id, request_hash, dead_letter_id,
                sequence_no, action, supersedes_disposition_id, reason_code,
                operator_id, expected_dead_letter_fingerprint,
                expected_candidate_version, evidence_ref, evidence_sha256,
                evidence_json, created_at
            )
            VALUES (
                'probe-disposition-revoke', 'probe-disposition-request-revoke',
                ?, 'probe-dead-letter-one', 2, 'REVOKE',
                'probe-disposition-one', 'PROBE_REVOKE', 'probe.operator', ?,
                ?, 'probe-revoke-evidence', ?, '{}',
                '2026-01-01T00:00:03Z'
            )
            """,
            ("5" * 64, "6" * 64, "7" * 64, "8" * 64),
        )
        results["dead_letter_disposition_request_unique"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO incremental_evaluation_dead_letter_dispositions (
                disposition_id, request_id, request_hash, dead_letter_id,
                sequence_no, action, reason_code, operator_id,
                expected_dead_letter_fingerprint, expected_candidate_version,
                evidence_ref, evidence_sha256, created_at
            ) VALUES (
                'probe-disposition-request-duplicate',
                'probe-disposition-request-one', ?, 'probe-dead-letter-one', 3,
                'REVOKE', 'PROBE_DUPLICATE', 'probe.operator', ?, ?,
                'probe-evidence', ?, '2026-01-01T00:00:04Z'
            )
            """,
            ("9" * 64, "a" * 64, "b" * 64, "c" * 64),
        )
        results["dead_letter_disposition_sequence_unique"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO incremental_evaluation_dead_letter_dispositions (
                disposition_id, request_id, request_hash, dead_letter_id,
                sequence_no, action, reason_code, operator_id,
                expected_dead_letter_fingerprint, expected_candidate_version,
                evidence_ref, evidence_sha256, created_at
            ) VALUES (
                'probe-disposition-sequence-duplicate',
                'probe-disposition-request-sequence-duplicate', ?,
                'probe-dead-letter-one', 1, 'REVOKE', 'PROBE_DUPLICATE',
                'probe.operator', ?, ?, 'probe-evidence', ?,
                '2026-01-01T00:00:04Z'
            )
            """,
            ("d" * 64, "e" * 64, "f" * 64, "0" * 64),
        )
        results["dead_letter_disposition_action_check"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO incremental_evaluation_dead_letter_dispositions (
                disposition_id, request_id, request_hash, dead_letter_id,
                sequence_no, action, reason_code, operator_id,
                expected_dead_letter_fingerprint, expected_candidate_version,
                evidence_ref, evidence_sha256, created_at
            ) VALUES (
                'probe-disposition-invalid-action',
                'probe-disposition-request-invalid-action', ?,
                'probe-dead-letter-one', 3, 'INVALID', 'PROBE_INVALID',
                'probe.operator', ?, ?, 'probe-evidence', ?,
                '2026-01-01T00:00:04Z'
            )
            """,
            ("0" * 64, "1" * 64, "2" * 64, "3" * 64),
        )
        results["dead_letter_disposition_sequence_positive"] = _expect_integrity_error(
            connection,
            """
            INSERT INTO incremental_evaluation_dead_letter_dispositions (
                disposition_id, request_id, request_hash, dead_letter_id,
                sequence_no, action, reason_code, operator_id,
                expected_dead_letter_fingerprint, expected_candidate_version,
                evidence_ref, evidence_sha256, created_at
            ) VALUES (
                'probe-disposition-sequence-zero',
                'probe-disposition-request-sequence-zero', ?,
                'probe-dead-letter-one', 0, 'REVOKE', 'PROBE_INVALID',
                'probe.operator', ?, ?, 'probe-evidence', ?,
                '2026-01-01T00:00:04Z'
            )
            """,
            ("4" * 64, "5" * 64, "6" * 64, "7" * 64),
        )
        results["dead_letter_disposition_dead_letter_foreign_key"] = _expect_integrity_error(
            connection,
            """
                INSERT INTO incremental_evaluation_dead_letter_dispositions (
                    disposition_id, request_id, request_hash, dead_letter_id,
                    sequence_no, action, reason_code, operator_id,
                    expected_dead_letter_fingerprint,
                    expected_candidate_version, evidence_ref, evidence_sha256,
                    created_at
                ) VALUES (
                    'probe-disposition-orphan',
                    'probe-disposition-request-orphan', ?,
                    'probe-dead-letter-missing', 1, 'REVOKE', 'PROBE_ORPHAN',
                    'probe.operator', ?, ?, 'probe-evidence', ?,
                    '2026-01-01T00:00:04Z'
                )
                """,
            ("8" * 64, "9" * 64, "a" * 64, "b" * 64),
        )
        results["dead_letter_disposition_supersedes_foreign_key"] = _expect_integrity_error(
            connection,
            """
                INSERT INTO incremental_evaluation_dead_letter_dispositions (
                    disposition_id, request_id, request_hash, dead_letter_id,
                    sequence_no, action, supersedes_disposition_id,
                    reason_code, operator_id,
                    expected_dead_letter_fingerprint,
                    expected_candidate_version, evidence_ref, evidence_sha256,
                    created_at
                ) VALUES (
                    'probe-disposition-supersedes-orphan',
                    'probe-disposition-request-supersedes-orphan', ?,
                    'probe-dead-letter-one', 3, 'REVOKE',
                    'probe-disposition-missing', 'PROBE_ORPHAN',
                    'probe.operator', ?, ?, 'probe-evidence', ?,
                    '2026-01-01T00:00:04Z'
                )
                """,
            ("c" * 64, "d" * 64, "e" * 64, "f" * 64),
        )
        results["dead_letter_disposition_observe_scope_check"] = _expect_integrity_error(
            connection,
            """
                INSERT INTO incremental_evaluation_dead_letter_dispositions (
                    disposition_id, request_id, request_hash, dead_letter_id,
                    sequence_no, action, reason_code, operator_id,
                    expected_dead_letter_fingerprint,
                    expected_candidate_version, evidence_ref, evidence_sha256,
                    created_at, observe_only
                ) VALUES (
                    'probe-disposition-observe-invalid',
                    'probe-disposition-request-observe-invalid', ?,
                    'probe-dead-letter-one', 3, 'REVOKE', 'PROBE_INVALID',
                    'probe.operator', ?, ?, 'probe-evidence', ?,
                    '2026-01-01T00:00:04Z', 0
                )
                """,
            ("1" * 64, "2" * 64, "3" * 64, "4" * 64),
        )
        results["dead_letter_disposition_canary_requires_fence"] = _expect_integrity_error(
            connection,
            """
                INSERT INTO incremental_evaluation_dead_letter_dispositions (
                    disposition_id, request_id, request_hash, dead_letter_id,
                    sequence_no, action, reason_code, operator_id,
                    expected_dead_letter_fingerprint,
                    expected_candidate_version, evidence_ref, evidence_sha256,
                    created_at
                ) VALUES (
                    'probe-disposition-canary-no-fence',
                    'probe-disposition-request-canary-no-fence', ?,
                    'probe-dead-letter-one', 3, 'RESET_CANARY',
                    'PROBE_INVALID', 'probe.operator', ?, ?,
                    'probe-evidence', ?, '2026-01-01T00:00:04Z'
                )
                """,
            ("5" * 64, "6" * 64, "7" * 64, "8" * 64),
        )
        results["dead_letter_disposition_canary_exactly_one"] = _expect_integrity_error(
            connection,
            """
                INSERT INTO incremental_evaluation_dead_letter_dispositions (
                    disposition_id, request_id, request_hash, dead_letter_id,
                    sequence_no, action, reason_code, operator_id,
                    expected_dead_letter_fingerprint,
                    expected_candidate_version, evidence_ref, evidence_sha256,
                    recovery_session_id, batch_size, fencing_token, created_at
                ) VALUES (
                    'probe-disposition-canary-two',
                    'probe-disposition-request-canary-two', ?,
                    'probe-dead-letter-one', 3, 'RESET_CANARY',
                    'PROBE_INVALID', 'probe.operator', ?, ?,
                    'probe-evidence', ?, 'probe-session', 2, 1,
                    '2026-01-01T00:00:04Z'
                )
                """,
            ("9" * 64, "a" * 64, "b" * 64, "c" * 64),
        )
        results["dead_letter_disposition_batch_max_five"] = _expect_integrity_error(
            connection,
            """
                INSERT INTO incremental_evaluation_dead_letter_dispositions (
                    disposition_id, request_id, request_hash, dead_letter_id,
                    sequence_no, action, reason_code, operator_id,
                    expected_dead_letter_fingerprint,
                    expected_candidate_version, evidence_ref, evidence_sha256,
                    recovery_session_id, batch_size, fencing_token, created_at
                ) VALUES (
                    'probe-disposition-batch-six',
                    'probe-disposition-request-batch-six', ?,
                    'probe-dead-letter-one', 3, 'RESET_BATCH',
                    'PROBE_INVALID', 'probe.operator', ?, ?,
                    'probe-evidence', ?, 'probe-session', 6, 1,
                    '2026-01-01T00:00:04Z'
                )
                """,
            ("d" * 64, "e" * 64, "f" * 64, "0" * 64),
        )
        results["incremental_dead_letter_update_blocked"] = _expect_integrity_error(
            connection,
            """
            UPDATE incremental_evaluation_dead_letters
            SET last_error = 'MUTATED'
            WHERE dead_letter_id = 'probe-dead-letter-one'
            """,
        )
        results["incremental_dead_letter_delete_blocked"] = _expect_integrity_error(
            connection,
            """
            DELETE FROM incremental_evaluation_dead_letters
            WHERE dead_letter_id = 'probe-dead-letter-one'
            """,
        )
        results["dead_letter_disposition_update_blocked"] = _expect_integrity_error(
            connection,
            """
            UPDATE incremental_evaluation_dead_letter_dispositions
            SET reason_code = 'MUTATED'
            WHERE disposition_id = 'probe-disposition-one'
            """,
        )
        results["dead_letter_disposition_delete_blocked"] = _expect_integrity_error(
            connection,
            """
            DELETE FROM incremental_evaluation_dead_letter_dispositions
            WHERE disposition_id = 'probe-disposition-one'
            """,
        )
        pipeline_hashes = tuple(character * 64 for character in "01234567")
        connection.execute(
            """
            INSERT INTO pipeline_coherency_dispositions (
                disposition_id, request_id, request_hash,
                candidate_instance_id, subject_key, trade_date, order_plan_id,
                sequence_no, action, reason_code, operator_id,
                expected_pipeline_fingerprint, expected_subject_version,
                expected_source_fingerprint, expected_candidate_fingerprint,
                expected_downstream_fingerprint, expected_boundary_fingerprint,
                evidence_type, evidence_ref, evidence_sha256, evidence_json,
                safety_snapshot_json, created_at
            ) VALUES (
                'probe-pipeline-disposition-one',
                'probe-pipeline-disposition-request-one', ?,
                'probe-pipeline-candidate', 'candidate:probe-pipeline-candidate',
                '2026-01-01', 'probe-plan', 1,
                'DISPOSE_EXPIRED_PLAN_READY', 'PROBE_EXPIRED',
                'probe.operator', ?, ?, ?, ?, ?, ?, 'PROBE_EVIDENCE',
                'probe-pipeline-evidence', ?, '{}', '{}',
                '2026-01-01T00:00:00Z'
            )
            """,
            pipeline_hashes,
        )
        results["pipeline_disposition_revoke_insert_supported"] = _expect_success(
            connection,
            """
            INSERT INTO pipeline_coherency_dispositions (
                disposition_id, request_id, request_hash,
                candidate_instance_id, subject_key, trade_date, order_plan_id,
                sequence_no, action, supersedes_disposition_id, reason_code,
                operator_id, expected_pipeline_fingerprint,
                expected_subject_version, expected_source_fingerprint,
                expected_candidate_fingerprint,
                expected_downstream_fingerprint,
                expected_boundary_fingerprint, evidence_type, evidence_ref,
                evidence_sha256, evidence_json, safety_snapshot_json, created_at
            ) VALUES (
                'probe-pipeline-disposition-revoke',
                'probe-pipeline-disposition-request-revoke', ?,
                'probe-pipeline-candidate', 'candidate:probe-pipeline-candidate',
                '2026-01-01', 'probe-plan', 2, 'REVOKE',
                'probe-pipeline-disposition-one', 'PROBE_REVOKE',
                'probe.operator', ?, ?, ?, ?, ?, ?, 'PROBE_EVIDENCE',
                'probe-pipeline-revoke-evidence', ?, '{}', '{}',
                '2026-01-01T00:00:01Z'
            )
            """,
            tuple(character * 64 for character in "89abcdef"),
        )
        pipeline_invalid_insert = """
            INSERT INTO pipeline_coherency_dispositions (
                disposition_id, request_id, request_hash,
                candidate_instance_id, subject_key, sequence_no, action,
                reason_code, operator_id, expected_pipeline_fingerprint,
                expected_subject_version, expected_source_fingerprint,
                expected_candidate_fingerprint,
                expected_downstream_fingerprint,
                expected_boundary_fingerprint, evidence_type, evidence_ref,
                evidence_sha256, created_at{extra_columns}
            ) VALUES (
                ?, ?, ?, 'probe-pipeline-candidate',
                'candidate:probe-pipeline-candidate', ?, ?, 'PROBE_INVALID',
                'probe.operator', ?, ?, ?, ?, ?, ?, 'PROBE_EVIDENCE',
                'probe-invalid-evidence', ?, '2026-01-01T00:00:02Z'{extra_values}
            )
        """
        base_invalid_params: tuple[object, ...] = (
            "probe-pipeline-invalid",
            "probe-pipeline-invalid-request",
            "0" * 64,
            3,
            "REVOKE",
            "1" * 64,
            "2" * 64,
            "3" * 64,
            "4" * 64,
            "5" * 64,
            "6" * 64,
            "7" * 64,
        )
        results["pipeline_disposition_request_unique"] = _expect_integrity_error(
            connection,
            pipeline_invalid_insert.format(extra_columns="", extra_values=""),
            (
                "probe-pipeline-request-duplicate",
                "probe-pipeline-disposition-request-one",
                *base_invalid_params[2:],
            ),
        )
        results["pipeline_disposition_sequence_unique"] = _expect_integrity_error(
            connection,
            pipeline_invalid_insert.format(extra_columns="", extra_values=""),
            (
                "probe-pipeline-sequence-duplicate",
                "probe-pipeline-sequence-duplicate-request",
                "0" * 64,
                1,
                *base_invalid_params[4:],
            ),
        )
        results["pipeline_disposition_action_check"] = _expect_integrity_error(
            connection,
            pipeline_invalid_insert.format(extra_columns="", extra_values=""),
            (
                "probe-pipeline-invalid-action",
                "probe-pipeline-invalid-action-request",
                "0" * 64,
                3,
                "INVALID",
                *base_invalid_params[5:],
            ),
        )
        results["pipeline_disposition_sequence_positive"] = _expect_integrity_error(
            connection,
            pipeline_invalid_insert.format(extra_columns="", extra_values=""),
            (
                "probe-pipeline-invalid-sequence",
                "probe-pipeline-invalid-sequence-request",
                "0" * 64,
                0,
                *base_invalid_params[4:],
            ),
        )
        pipeline_hash_parameter_indexes = {
            "request_hash": 2,
            "expected_pipeline_fingerprint": 5,
            "expected_subject_version": 6,
            "expected_source_fingerprint": 7,
            "expected_candidate_fingerprint": 8,
            "expected_downstream_fingerprint": 9,
            "expected_boundary_fingerprint": 10,
            "evidence_sha256": 11,
        }
        invalid_hash_values: tuple[tuple[str, object], ...] = (
            ("lower_hex", "A" * 64),
            ("length", "a" * 63),
            ("text_type", sqlite3.Binary(b"\xab" * 32)),
        )
        for column, parameter_index in pipeline_hash_parameter_indexes.items():
            for check_name, invalid_value in invalid_hash_values:
                probe_params = list(base_invalid_params)
                probe_key = f"{column}-{check_name}"
                probe_params[0] = f"probe-pipeline-invalid-{probe_key}"
                probe_params[1] = f"probe-pipeline-invalid-{probe_key}-request"
                probe_params[parameter_index] = invalid_value
                results[f"pipeline_disposition_{column}_{check_name}_check"] = (
                    _expect_integrity_error(
                        connection,
                        pipeline_invalid_insert.format(extra_columns="", extra_values=""),
                        tuple(probe_params),
                    )
                )
        results["pipeline_disposition_lower_hex_hash_check"] = results[
            "pipeline_disposition_request_hash_lower_hex_check"
        ]
        results["pipeline_disposition_supersedes_foreign_key"] = _expect_integrity_error(
            connection,
            pipeline_invalid_insert.format(
                extra_columns=", supersedes_disposition_id",
                extra_values=", 'probe-pipeline-disposition-missing'",
            ),
            (
                "probe-pipeline-invalid-supersedes",
                "probe-pipeline-invalid-supersedes-request",
                *base_invalid_params[2:],
            ),
        )
        for column, invalid_value in (
            ("observe_only", 0),
            ("live_sim_allowed", 1),
            ("live_real_allowed", 1),
            ("order_commands_allowed", 1),
            ("not_order_intent", 0),
            ("no_order_side_effects", 0),
            ("auto_run_evaluation", 1),
        ):
            results[f"pipeline_disposition_{column}_check"] = _expect_integrity_error(
                connection,
                pipeline_invalid_insert.format(
                    extra_columns=f", {column}",
                    extra_values=", ?",
                ),
                (
                    f"probe-pipeline-invalid-{column}",
                    f"probe-pipeline-invalid-{column}-request",
                    *base_invalid_params[2:],
                    invalid_value,
                ),
            )
        results["pipeline_disposition_update_blocked"] = _expect_integrity_error(
            connection,
            """
            UPDATE pipeline_coherency_dispositions
            SET reason_code = 'MUTATED'
            WHERE disposition_id = 'probe-pipeline-disposition-one'
            """,
        )
        results["pipeline_disposition_delete_blocked"] = _expect_integrity_error(
            connection,
            """
            DELETE FROM pipeline_coherency_dispositions
            WHERE disposition_id = 'probe-pipeline-disposition-one'
            """,
        )
    finally:
        connection.execute("ROLLBACK TO target_contract_probe")
        connection.execute("RELEASE target_contract_probe")
        connection.close()
    return results


def _expect_integrity_error(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[object, ...] = (),
) -> bool:
    try:
        connection.execute(sql, params)
    except sqlite3.IntegrityError:
        return True
    return False


def _expect_success(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[object, ...] = (),
) -> bool:
    try:
        connection.execute(sql, params)
    except sqlite3.Error:
        return False
    return True


def _table_content_fingerprints(
    connection: sqlite3.Connection,
    *,
    tables: set[str],
) -> dict[str, dict[str, Any]]:
    fingerprints: dict[str, dict[str, Any]] = {}
    for table_name in sorted(tables):
        column_rows = connection.execute(
            f"PRAGMA table_info({_quote_identifier(table_name)})"
        ).fetchall()
        columns = [str(row["name"]) for row in column_rows]
        digest = hashlib.sha256()
        digest.update(
            json.dumps(columns, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        row_count = 0
        if columns:
            column_sql = ", ".join(_quote_identifier(column) for column in columns)
            primary_key_columns = [
                str(row["name"])
                for row in sorted(
                    (row for row in column_rows if int(row["pk"] or 0) > 0),
                    key=lambda row: int(row["pk"]),
                )
            ]
            table_row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            table_sql = "" if table_row is None else str(table_row["sql"] or "")
            include_rowid = "WITHOUT ROWID" not in table_sql.upper()
            order_sql = (
                "_rowid_"
                if include_rowid
                else ", ".join(_quote_identifier(column) for column in primary_key_columns)
            )
            select_sql = (
                f"_rowid_ AS {_quote_identifier('__codex_rowid__')}, {column_sql}"
                if include_rowid
                else column_sql
            )
            where_sql = (
                "WHERE key <> 'schema_version'"
                if table_name == "app_metadata" and "key" in columns
                else ""
            )
            query = (
                f"SELECT {select_sql} FROM {_quote_identifier(table_name)} "
                f"{where_sql} "
                f"ORDER BY {order_sql}"
            )
            for row in connection.execute(query):
                values = [_sql_value_for_hash(row[column]) for column in columns]
                if include_rowid:
                    values.insert(
                        0,
                        {
                            "type": "rowid",
                            "value": int(row["__codex_rowid__"]),
                        },
                    )
                encoded = json.dumps(
                    values,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
                row_count += 1
        fingerprints[table_name] = {
            "row_count": row_count,
            "sha256": digest.hexdigest(),
        }
    return fingerprints


def _sql_value_for_hash(value: object) -> dict[str, Any]:
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, bytes):
        return {
            "type": "blob",
            "value": hashlib.sha256(value).hexdigest(),
            "size": len(value),
        }
    if isinstance(value, float):
        return {"type": "float", "value": value.hex()}
    if isinstance(value, int):
        return {"type": "integer", "value": value}
    return {"type": "text", "value": str(value)}


def _changed_table_content(
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    excluded: frozenset[str] = frozenset(),
) -> list[str]:
    return sorted(
        table_name
        for table_name, fingerprint in before.items()
        if table_name not in excluded and after.get(table_name) != fingerprint
    )


def _quick_check(path: Path) -> list[str]:
    connection = sqlite3.connect(path, timeout=60.0)
    connection.execute("PRAGMA query_only=ON")
    try:
        return [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
    finally:
        connection.close()


def _file_fingerprints(path: Path) -> dict[str, dict[str, Any]]:
    fingerprints: dict[str, dict[str, Any]] = {}
    for label, suffix in FINGERPRINT_FILES.items():
        candidate = Path(f"{path}{suffix}")
        if not candidate.exists():
            fingerprints[label] = {
                "exists": False,
                "size": 0,
                "mtime_ns": None,
                "sha256": None,
            }
            continue
        stat = candidate.stat()
        fingerprints[label] = {
            "exists": True,
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "sha256": _sha256_file(candidate),
        }
    return fingerprints


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _changed_source_data_files(
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> list[str]:
    changed: list[str] = []
    for label, display_name in (
        ("main", "<main>"),
        ("wal", "-wal"),
        ("shm", "-shm"),
        ("journal", "-journal"),
    ):
        if before.get(label) != after.get(label):
            changed.append(display_name)
    return changed


def _backup_progress_printer():
    next_percent = 0

    def progress(_status: int, remaining: int, total: int) -> None:
        nonlocal next_percent
        if total <= 0:
            return
        percent = int(((total - remaining) * 100) / total)
        if percent < next_percent and remaining:
            return
        print(
            f"backup_progress={percent}% remaining_pages={remaining} total_pages={total}",
            flush=True,
        )
        next_percent = min(percent + 5, 100)

    return progress


def _write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir / stamp
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(_render_markdown(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def _render_markdown(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    source = _mapping(report.get("source"))
    clone = _mapping(report.get("clone"))
    source_snapshot = _mapping(source.get("snapshot"))
    clone_after = _mapping(clone.get("after_migration"))
    return "\n".join(
        (
            "# Database Migration Preflight",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- preflight_contract_version: `{report.get('preflight_contract_version')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- source_db: `{source.get('path')}`",
            f"- clone_db: `{clone.get('path')}`",
            "- schema: "
            f"`{source_snapshot.get('schema_version')} -> "
            f"{clone_after.get('schema_version')}`",
            f"- backup_elapsed_sec: `{float(clone.get('backup_elapsed_sec') or 0):.3f}`",
            f"- migration_elapsed_sec: `{float(clone.get('migration_elapsed_sec') or 0):.3f}`",
            f"- migration_method: `{clone.get('migration_method')}`",
            f"- exact_quick_check: `{json.dumps(clone.get('exact_quick_check'))}`",
            f"- idempotent_elapsed_sec: `{float(clone.get('idempotent_elapsed_sec') or 0):.3f}`",
            f"- quick_check_elapsed_sec: `{float(clone.get('quick_check_elapsed_sec') or 0):.3f}`",
            f"- quick_check: `{json.dumps(clone.get('quick_check'))}`",
            "- projection_outbox_before: "
            f"`{json.dumps(source_snapshot.get('projection_outbox'), sort_keys=True)}`",
            "- projection_outbox_after: "
            f"`{json.dumps(clone_after.get('projection_outbox'), sort_keys=True)}`",
            "- source_data_files_unchanged: "
            f"`{str(bool(verdict.get('source_data_files_unchanged'))).lower()}`",
            "- exact_migration_target_valid: "
            f"`{str(bool(verdict.get('exact_migration_target_valid'))).lower()}`",
            "- exact_target_table_set_valid: "
            f"`{str(bool(verdict.get('exact_target_table_set_valid'))).lower()}`",
            "- probe_rollback_no_change: "
            f"`{str(bool(verdict.get('probe_rollback_no_change'))).lower()}`",
            "- idempotent_rerun_no_change: "
            f"`{str(bool(verdict.get('idempotent_rerun_no_change'))).lower()}`",
            "- required_tables_present: "
            f"`{str(bool(verdict.get('required_tables_present'))).lower()}`",
            "- required_columns_present: "
            f"`{str(bool(verdict.get('required_columns_present'))).lower()}`",
            "- required_indexes_present: "
            f"`{str(bool(verdict.get('required_indexes_present'))).lower()}`",
            "- required_append_only_triggers_present: "
            f"`{str(bool(verdict.get('required_append_only_triggers_present'))).lower()}`",
            "- target_dead_letter_disposition_ledger_empty: "
            f"`{str(bool(verdict.get('target_dead_letter_disposition_ledger_empty'))).lower()}`",
            f"- failures: `{json.dumps(verdict.get('failures') or [])}`",
            "",
            "The source database was opened with SQLite `mode=ro` and "
            "`PRAGMA query_only=ON`. Migration and integrity work ran only on the clone.",
        )
    )


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
