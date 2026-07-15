from __future__ import annotations

import argparse
import hashlib
import json
import os
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
    PIPELINE_QUALIFICATION_INDEXES,
    PIPELINE_QUALIFICATION_TABLE,
    PIPELINE_QUALIFICATION_TRIGGERS,
    SCHEMA_VERSION,
    migrate_schema_61_to_62,
)
from tools.ops_database_migration_preflight import (  # noqa: E402
    EXACT_SCHEMA_61_TO_62_MIGRATION_METHOD,
    PREFLIGHT_CONTRACT_VERSION,
    REQUIRED_TARGET_COLUMNS,
    REQUIRED_TARGET_INDEX_CONTRACTS,
    REQUIRED_TARGET_TABLES,
    REQUIRED_TARGET_TRIGGER_CONTRACTS,
    _database_snapshot,
    _file_fingerprints,
    _open_read_only,
    evaluate_report,
)

SOURCE_SCHEMA = "61"
TARGET_SCHEMA = "62"
APPLY_ACKNOWLEDGEMENT = "APPLY_EXACT_SCHEMA_61_TO_62_PIPELINE_LEDGER"
MAX_PREFLIGHT_AGE_SEC = 3_600
MAX_FUTURE_SKEW_SEC = 300
SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
MIN_FREE_MARGIN_BYTES = 256 * 1024 * 1024
REQUIRED_PIPELINE_PREFLIGHT_PROBES = frozenset(
    {
        "pipeline_disposition_revoke_insert_supported",
        "pipeline_disposition_request_unique",
        "pipeline_disposition_sequence_unique",
        "pipeline_disposition_action_check",
        "pipeline_disposition_sequence_positive",
        "pipeline_disposition_lower_hex_hash_check",
        "pipeline_disposition_supersedes_foreign_key",
        "pipeline_disposition_observe_only_check",
        "pipeline_disposition_live_sim_allowed_check",
        "pipeline_disposition_live_real_allowed_check",
        "pipeline_disposition_order_commands_allowed_check",
        "pipeline_disposition_not_order_intent_check",
        "pipeline_disposition_no_order_side_effects_check",
        "pipeline_disposition_auto_run_evaluation_check",
        "pipeline_disposition_update_blocked",
        "pipeline_disposition_delete_blocked",
    }
)


class MigrationApplyError(RuntimeError):
    def __init__(self, code: str, *, committed: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.committed = committed


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Apply only the preflight-qualified SQLite schema 61 -> 62 "
            "append-only pipeline ledger migration."
        )
    )
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--preflight-raw", required=True)
    parser.add_argument("--backup-db", required=True)
    parser.add_argument("--acknowledge", required=True)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "database_migration_apply"),
    )
    args = parser.parse_args()

    try:
        report = run_apply(
            source_db=Path(args.source_db),
            preflight_raw=Path(args.preflight_raw),
            backup_db=Path(args.backup_db),
            acknowledge=args.acknowledge,
            out_dir=Path(args.out_dir),
        )
    except MigrationApplyError as exc:
        _emit_failure(
            exc,
            source_db=Path(args.source_db),
            backup_db=Path(args.backup_db),
            preflight_raw=Path(args.preflight_raw),
            out_dir=Path(args.out_dir),
        )
        return 2
    except Exception as exc:  # pragma: no cover - final fail-closed CLI boundary
        wrapped = MigrationApplyError(
            f"UNEXPECTED_STATE_UNKNOWN_{type(exc).__name__.upper()}",
            committed=True,
        )
        _emit_failure(
            wrapped,
            source_db=Path(args.source_db),
            backup_db=Path(args.backup_db),
            preflight_raw=Path(args.preflight_raw),
            out_dir=Path(args.out_dir),
        )
        return 2

    print(
        "database migration apply: PASS schema=61->62 "
        f"backup_sha256={report['backup']['sha256']} "
        f"raw={report['report_paths']['raw_json']}",
        flush=True,
    )
    return 0


def run_apply(
    *,
    source_db: Path,
    preflight_raw: Path,
    backup_db: Path,
    acknowledge: str,
    out_dir: Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    source = source_db.expanduser().resolve()
    preflight_path = preflight_raw.expanduser().resolve()
    backup = backup_db.expanduser().resolve()

    if acknowledge != APPLY_ACKNOWLEDGEMENT:
        raise MigrationApplyError("EXACT_ACKNOWLEDGEMENT_REQUIRED")
    if str(SCHEMA_VERSION) != TARGET_SCHEMA:
        raise MigrationApplyError("CURRENT_CODE_TARGET_SCHEMA_MISMATCH")
    _validate_paths(source=source, preflight_raw=preflight_path, backup=backup)

    preflight_bytes = preflight_path.read_bytes()
    preflight_sha256 = hashlib.sha256(preflight_bytes).hexdigest()
    try:
        loaded = json.loads(preflight_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationApplyError("PREFLIGHT_RAW_INVALID") from exc
    preflight = _mapping(loaded)
    preflight_age_sec = _validate_preflight(preflight, source=source)
    expected_source_snapshot = _mapping(_mapping(preflight.get("source")).get("snapshot"))
    expected_source_files = _mapping(_mapping(preflight.get("source")).get("files_after"))

    _assert_no_sidecars(source, code="SOURCE_SIDECAR_PRESENT")
    _assert_single_link(source, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
    current_files = _file_fingerprints(source)
    if current_files != expected_source_files:
        raise MigrationApplyError("SOURCE_FINGERPRINT_CHANGED_SINCE_PREFLIGHT")
    disk = _disk_contract(source=source, backup=backup)
    if disk["sufficient"] is not True:
        raise MigrationApplyError("BACKUP_DISK_SPACE_INSUFFICIENT")

    ro_started = time.perf_counter()
    source_ro = _open_read_only(source)
    try:
        source_quick_check = [str(row[0]) for row in source_ro.execute("PRAGMA quick_check(1)")]
        if source_quick_check != ["ok"]:
            raise MigrationApplyError("SOURCE_QUICK_CHECK_FAILED")
        fresh_source_snapshot = _database_snapshot(source_ro)
        lease_counts = _lease_counts(source_ro)
    finally:
        source_ro.close()
    ro_elapsed_sec = time.perf_counter() - ro_started
    if fresh_source_snapshot != expected_source_snapshot:
        raise MigrationApplyError("SOURCE_TYPED_SNAPSHOT_CHANGED_SINCE_PREFLIGHT")
    _assert_zero_leases(lease_counts)
    if _file_fingerprints(source) != expected_source_files:
        raise MigrationApplyError("SOURCE_CHANGED_DURING_READ_ONLY_RECHECK")
    _assert_no_sidecars(source, code="SOURCE_SIDECAR_APPEARED_DURING_RECHECK")

    backup.parent.mkdir(parents=True, exist_ok=True)
    connection = _open_read_write(source)
    committed = False
    precommit_error: MigrationApplyError | None = None
    authorizer_denials: list[tuple[int, str | None, str | None]] = []
    contract_probes: dict[str, bool] = {}
    precommit_snapshot: dict[str, Any] = {}
    try:
        connection.execute("BEGIN EXCLUSIVE")
        exclusive_leases = _lease_counts(connection)
        _assert_zero_leases(exclusive_leases)
        if _main_fingerprint(source) != _mapping(expected_source_files.get("main")):
            raise MigrationApplyError("SOURCE_CHANGED_BEFORE_EXCLUSIVE_APPLY")

        _copy_byte_identical(source=source, backup=backup)
        _assert_single_link(backup, code="BACKUP_HARDLINK_COUNT_NOT_ONE")
        backup_fingerprint = _main_fingerprint(backup)
        expected_main = _mapping(expected_source_files.get("main"))
        if backup_fingerprint.get("size") != expected_main.get("size") or backup_fingerprint.get(
            "sha256"
        ) != expected_main.get("sha256"):
            raise MigrationApplyError("BACKUP_NOT_BYTE_IDENTICAL")
        backup_quick_check, backup_snapshot = _strict_read_only_snapshot(backup)
        if backup_quick_check != ["ok"]:
            raise MigrationApplyError("BACKUP_QUICK_CHECK_FAILED")
        if backup_snapshot != expected_source_snapshot:
            raise MigrationApplyError("BACKUP_TYPED_SNAPSHOT_MISMATCH")
        _assert_no_sidecars(backup, code="BACKUP_SIDECAR_PRESENT")

        connection.set_authorizer(_migration_authorizer(authorizer_denials))
        try:
            migrate_schema_61_to_62(connection)
        except sqlite3.DatabaseError as exc:
            raise MigrationApplyError("MIGRATION_AUTHORIZER_OR_SQL_FAILURE") from exc
        finally:
            connection.set_authorizer(None)
        if authorizer_denials:
            raise MigrationApplyError("MIGRATION_AUTHORIZER_DENIED_OPERATION")

        contract_probes = _probe_pipeline_ledger_contract(connection)
        if not contract_probes or not all(contract_probes.values()):
            raise MigrationApplyError("TARGET_PIPELINE_LEDGER_PROBE_FAILED")
        precommit_snapshot = _database_snapshot(connection)
        _assert_target_snapshot(
            precommit_snapshot,
            source_snapshot=expected_source_snapshot,
        )
        if _main_fingerprint(backup) != backup_fingerprint:
            raise MigrationApplyError("BACKUP_CHANGED_BEFORE_COMMIT")
        connection.commit()
        committed = True
    except MigrationApplyError as exc:
        precommit_error = exc
        try:
            if connection.in_transaction:
                connection.rollback()
        except Exception:
            precommit_error = MigrationApplyError("ROLLBACK_FAILED")
    except Exception as exc:
        precommit_error = MigrationApplyError("UNEXPECTED_PRECOMMIT_FAILURE")
        precommit_error.__cause__ = exc
        try:
            if connection.in_transaction:
                connection.rollback()
        except Exception:
            precommit_error = MigrationApplyError("ROLLBACK_FAILED")
    finally:
        try:
            connection.set_authorizer(None)
        except Exception:
            if precommit_error is None:
                precommit_error = MigrationApplyError("AUTHORIZER_CLEANUP_FAILED")
        try:
            connection.close()
        except Exception:
            if precommit_error is None:
                precommit_error = MigrationApplyError("CONNECTION_CLOSE_FAILED")

    if precommit_error is not None:
        reconciled_state = _reconcile_apply_state(
            source,
            expected_source_files=expected_source_files,
            expected_source_snapshot=expected_source_snapshot,
        )
        if reconciled_state == "SOURCE_61_UNCHANGED":
            raise precommit_error
        if reconciled_state == "TARGET_62_APPLIED":
            raise MigrationApplyError(precommit_error.code, committed=True) from precommit_error
        raise MigrationApplyError("MIGRATION_STATE_AMBIGUOUS", committed=True) from precommit_error

    try:
        _assert_no_sidecars(source, code="SOURCE_SIDECAR_LEFT_AFTER_COMMIT")
        _assert_no_sidecars(backup, code="BACKUP_SIDECAR_LEFT_AFTER_COMMIT")
        _assert_single_link(source, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
        _assert_single_link(backup, code="BACKUP_HARDLINK_COUNT_NOT_ONE")
        post_quick_check, post_snapshot = _strict_read_only_snapshot(source)
        if post_quick_check != ["ok"]:
            raise MigrationApplyError("POST_COMMIT_QUICK_CHECK_FAILED", committed=True)
        _assert_target_snapshot(post_snapshot, source_snapshot=expected_source_snapshot)
        exact_precommit_postcommit_snapshot_equal = _logical_snapshots_equal(
            post_snapshot,
            precommit_snapshot,
        )
        if not exact_precommit_postcommit_snapshot_equal:
            raise MigrationApplyError("POST_COMMIT_SNAPSHOT_MISMATCH", committed=True)
        (
            locked_post_quick_check,
            locked_post_snapshot,
            locked_post_main,
            locked_post_leases,
        ) = _exclusive_postcommit_snapshot(source)
        if locked_post_quick_check != ["ok"]:
            raise MigrationApplyError(
                "EXCLUSIVE_POST_COMMIT_QUICK_CHECK_FAILED",
                committed=True,
            )
        _assert_zero_leases(locked_post_leases)
        _assert_target_snapshot(
            locked_post_snapshot,
            source_snapshot=expected_source_snapshot,
        )
        postcommit_readonly_exclusive_snapshot_equal = _logical_snapshots_equal(
            locked_post_snapshot,
            post_snapshot,
        )
        if not postcommit_readonly_exclusive_snapshot_equal:
            raise MigrationApplyError(
                "EXCLUSIVE_POST_COMMIT_SNAPSHOT_MISMATCH",
                committed=True,
            )
        _assert_no_sidecars(source, code="SOURCE_SIDECAR_LEFT_AFTER_FINAL_CHECK")
        _assert_single_link(source, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
        post_files = _file_fingerprints(source)
        exclusive_postcommit_final_fingerprint_equal = (
            _mapping(post_files.get("main")) == locked_post_main
        )
        if not exclusive_postcommit_final_fingerprint_equal:
            raise MigrationApplyError(
                "SOURCE_FINGERPRINT_CHANGED_AFTER_FINAL_SNAPSHOT",
                committed=True,
            )
        _assert_no_sidecars(source, code="SOURCE_SIDECAR_APPEARED_AFTER_FINAL_HASH")
        _assert_single_link(source, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
        if _main_fingerprint(backup) != backup_fingerprint:
            raise MigrationApplyError("BACKUP_CHANGED_AFTER_COMMIT", committed=True)
        _assert_no_sidecars(backup, code="BACKUP_SIDECAR_APPEARED_AFTER_FINAL_HASH")
        _assert_single_link(backup, code="BACKUP_HARDLINK_COUNT_NOT_ONE")
    except MigrationApplyError as exc:
        if committed and not exc.committed:
            raise MigrationApplyError(exc.code, committed=True) from exc
        raise
    except Exception as exc:
        raise MigrationApplyError("UNEXPECTED_POSTCOMMIT_FAILURE", committed=True) from exc

    try:
        source_hardlink_count = int(source.stat().st_nlink)
        backup_stat = backup.stat()
        backup_sha256 = str(backup_fingerprint["sha256"])
        existing_table_count = len(_mapping(expected_source_snapshot["table_content"]))
        target_ledger_row_count = int(
            _mapping(post_snapshot["table_content"])[PIPELINE_QUALIFICATION_TABLE]["row_count"]
        )
    except Exception as exc:
        raise MigrationApplyError("EVIDENCE_BUILD_FAILED", committed=True) from exc

    report: dict[str, Any] = {
        "generated_at": _now(),
        "status": "PASS",
        "committed": True,
        "migration": {
            "source_schema": SOURCE_SCHEMA,
            "target_schema": TARGET_SCHEMA,
            "exact_function": "storage.sqlite.migrate_schema_61_to_62",
            "acknowledgement_matched": True,
            "authorizer_denial_count": len(authorizer_denials),
        },
        "source": {
            "path": str(source),
            "preflight_main": _public_fingerprint(_mapping(expected_source_files["main"])),
            "post_main": _public_fingerprint(_mapping(post_files["main"])),
            "hardlink_count": source_hardlink_count,
            "strict_read_only_quick_check": source_quick_check,
            "post_commit_quick_check": post_quick_check,
            "exclusive_post_commit_quick_check": locked_post_quick_check,
            "exclusive_post_commit_main": _public_fingerprint(locked_post_main),
            "strict_read_only_elapsed_sec": round(ro_elapsed_sec, 6),
            "schema_before": SOURCE_SCHEMA,
            "schema_after": str(post_snapshot.get("schema_version")),
            "sidecar_count_after": _sidecar_count(source),
        },
        "backup": {
            "path": str(backup),
            "size": int(backup_stat.st_size),
            "sha256": backup_sha256,
            "byte_identical": True,
            "hardlink_count": int(backup_stat.st_nlink),
            "quick_check": backup_quick_check,
            "schema_version": str(backup_snapshot.get("schema_version")),
            "sidecar_count": _sidecar_count(backup),
        },
        "preflight": {
            "path": str(preflight_path),
            "sha256": preflight_sha256,
            "generated_at": str(preflight.get("generated_at")),
            "age_sec": round(preflight_age_sec, 6),
            "verdict": "PASS",
        },
        "safety": {
            "disk": disk,
            "lease_counts": lease_counts,
            "exclusive_lease_counts": exclusive_leases,
            "exclusive_post_commit_lease_counts": locked_post_leases,
            "exclusive_post_commit_runtime_lease_count": int(
                locked_post_leases["runtime_execution_locks"]
            ),
            "exclusive_transaction_acquired": True,
            "exclusive_post_commit_transaction_acquired": True,
            "exact_precommit_postcommit_snapshot_equal": (
                exact_precommit_postcommit_snapshot_equal
            ),
            "postcommit_readonly_exclusive_snapshot_equal": (
                postcommit_readonly_exclusive_snapshot_equal
            ),
            "exclusive_postcommit_final_fingerprint_equal": (
                exclusive_postcommit_final_fingerprint_equal
            ),
            "source_sidecar_count_before": 0,
            "existing_table_content_preserved": True,
            "sqlite_sequence_preserved": True,
            "projection_outbox_preserved": True,
            "target_ledger_empty": True,
            "target_contract_probes": contract_probes,
        },
        "counts": {
            "existing_table_count": existing_table_count,
            "target_ledger_row_count": target_ledger_row_count,
        },
        "evidence_policy": {
            "hashes_and_counts_only": True,
            "raw_rows_included": False,
            "secrets_included": False,
            "account_numbers_included": False,
        },
        "elapsed_sec": round(time.perf_counter() - started, 6),
    }
    try:
        report_paths = _write_report(report, out_dir=out_dir)
    except Exception as exc:
        raise MigrationApplyError("EVIDENCE_WRITE_FAILED", committed=True) from exc
    report["report_paths"] = {name: str(path) for name, path in report_paths.items()}
    return report


def _validate_paths(*, source: Path, preflight_raw: Path, backup: Path) -> None:
    if not source.is_file():
        raise MigrationApplyError("SOURCE_NOT_FOUND")
    if not preflight_raw.is_file():
        raise MigrationApplyError("PREFLIGHT_RAW_NOT_FOUND")
    source_artifacts = {source, *(Path(f"{source}{suffix}") for suffix in SIDECAR_SUFFIXES)}
    if source == preflight_raw or backup in source_artifacts or preflight_raw == backup:
        raise MigrationApplyError("PATHS_MUST_BE_DISTINCT")
    if any(Path(f"{backup}{suffix}").exists() for suffix in ("", *SIDECAR_SUFFIXES)):
        raise MigrationApplyError("BACKUP_ARTIFACT_ALREADY_EXISTS")


def _validate_preflight(report: Mapping[str, Any], *, source: Path) -> float:
    verdict = _mapping(report.get("verdict"))
    recomputed_verdict = evaluate_report(report)
    source_report = _mapping(report.get("source"))
    clone = _mapping(report.get("clone"))
    source_snapshot = _mapping(source_report.get("snapshot"))
    clone_after = _mapping(clone.get("after_exact_migration"))
    source_before = _mapping(source_report.get("files_before"))
    source_after = _mapping(source_report.get("files_after"))
    probes = _mapping(clone.get("contract_probes"))
    target_required_tables = _mapping(clone_after.get("required_tables"))
    target_required_columns = _mapping(clone_after.get("required_columns"))
    target_required_indexes = _mapping(clone_after.get("required_indexes"))
    target_required_triggers = _mapping(clone_after.get("required_triggers"))

    required_true = (
        verdict.get("source_data_files_unchanged"),
        verdict.get("outbox_preserved"),
        verdict.get("sqlite_sequence_preserved"),
        verdict.get("backup_table_content_preserved"),
        verdict.get("migration_table_content_preserved"),
        verdict.get("backup_schema_objects_preserved"),
        verdict.get("backup_schema_version_record_preserved"),
        verdict.get("exact_snapshot_alias_valid"),
        verdict.get("exact_target_table_set_valid"),
        verdict.get("exact_migration_target_valid"),
        verdict.get("probe_rollback_no_change"),
        verdict.get("idempotent_rerun_no_change"),
        verdict.get("idempotent_schema_unchanged"),
        verdict.get("idempotent_table_content_unchanged"),
        verdict.get("target_behavior_contract_valid"),
        verdict.get("target_pipeline_disposition_ledger_empty"),
        verdict.get("required_tables_present"),
        verdict.get("required_columns_present"),
        verdict.get("required_indexes_present"),
        verdict.get("required_append_only_triggers_present"),
    )
    if (
        verdict.get("status") != "PASS"
        or verdict != recomputed_verdict
        or verdict.get("failures") != []
        or report.get("preflight_contract_version") != PREFLIGHT_CONTRACT_VERSION
        or report.get("required_source_schema") != SOURCE_SCHEMA
        or report.get("target_schema_version") != TARGET_SCHEMA
        or source_snapshot.get("schema_version") != SOURCE_SCHEMA
        or clone_after.get("schema_version") != TARGET_SCHEMA
        or clone.get("migration_method") != EXACT_SCHEMA_61_TO_62_MIGRATION_METHOD
        or _mapping(clone.get("after_migration")) != clone_after
        or clone.get("exact_quick_check") != ["ok"]
        or source_report.get("quick_check") != ["ok"]
        or clone.get("quick_check") != ["ok"]
        or source_report.get("opened_read_only") is not True
        or source_report.get("query_only") is not True
        or source_report.get("immutable") is not True
        or source_before != source_after
        or not all(value is True for value in required_true)
        or verdict.get("operating_database_mutated") is not False
        or not all(probes.get(name) is True for name in REQUIRED_PIPELINE_PREFLIGHT_PROBES)
        or target_required_tables.get(PIPELINE_QUALIFICATION_TABLE) is not True
        or _mapping(target_required_columns.get(PIPELINE_QUALIFICATION_TABLE)).get("valid")
        is not True
        or not all(
            _mapping(target_required_indexes.get(name)).get("valid") is True
            for name in PIPELINE_QUALIFICATION_INDEXES
        )
        or not all(
            _mapping(target_required_triggers.get(name)).get("valid") is True
            for name in PIPELINE_QUALIFICATION_TRIGGERS
        )
    ):
        raise MigrationApplyError("PREFLIGHT_CONTRACT_NOT_PASS")
    if Path(str(source_report.get("path", ""))).expanduser().resolve() != source:
        raise MigrationApplyError("PREFLIGHT_SOURCE_PATH_MISMATCH")

    source_tables = _mapping(source_snapshot.get("table_content"))
    target_tables = _mapping(clone_after.get("table_content"))
    if PIPELINE_QUALIFICATION_TABLE in source_tables:
        raise MigrationApplyError("PREFLIGHT_SOURCE_TARGET_TABLE_PRESENT")
    target_row_count = _mapping(target_tables.get(PIPELINE_QUALIFICATION_TABLE)).get("row_count")
    if target_row_count is None or int(target_row_count) != 0:
        raise MigrationApplyError("PREFLIGHT_TARGET_LEDGER_NOT_EMPTY")

    generated_at = _parse_timestamp(report.get("generated_at"))
    age_sec = (datetime.now(UTC) - generated_at).total_seconds()
    if age_sec < -MAX_FUTURE_SKEW_SEC:
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_IN_FUTURE")
    if age_sec > MAX_PREFLIGHT_AGE_SEC:
        raise MigrationApplyError("PREFLIGHT_STALE")
    return max(age_sec, 0.0)


def _open_read_write(path: Path) -> sqlite3.Connection:
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=rw",
        uri=True,
        timeout=5.0,
        isolation_level=None,
    )
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA foreign_keys=ON")
    except Exception:
        connection.close()
        raise
    return connection


def _migration_authorizer(
    denials: list[tuple[int, str | None, str | None]],
):
    allowed_indexes = set(PIPELINE_QUALIFICATION_INDEXES)
    allowed_triggers = set(PIPELINE_QUALIFICATION_TRIGGERS)
    autoindex_prefix = f"sqlite_autoindex_{PIPELINE_QUALIFICATION_TABLE}_"
    read_actions = {
        sqlite3.SQLITE_READ,
        sqlite3.SQLITE_SELECT,
        sqlite3.SQLITE_FUNCTION,
    }

    def authorize(
        action: int,
        arg1: str | None,
        arg2: str | None,
        _database: str | None,
        _trigger: str | None,
    ) -> int:
        allowed = False
        if action in read_actions:
            allowed = True
        elif action == sqlite3.SQLITE_PRAGMA:
            allowed = arg1 == "database_list"
        elif action == sqlite3.SQLITE_CREATE_TABLE:
            allowed = arg1 == PIPELINE_QUALIFICATION_TABLE
        elif action == sqlite3.SQLITE_CREATE_INDEX:
            allowed = bool(
                arg2 == PIPELINE_QUALIFICATION_TABLE
                and arg1 is not None
                and (arg1 in allowed_indexes or arg1.startswith(autoindex_prefix))
            )
        elif action == sqlite3.SQLITE_CREATE_TRIGGER:
            allowed = arg1 in allowed_triggers and arg2 == PIPELINE_QUALIFICATION_TABLE
        elif action == sqlite3.SQLITE_INSERT:
            allowed = arg1 == "sqlite_master"
        elif action == sqlite3.SQLITE_UPDATE:
            allowed = (
                arg1 == "sqlite_master" and arg2 in {"type", "name", "tbl_name", "rootpage", "sql"}
            ) or (arg1 == "app_metadata" and arg2 in {"value", "updated_at"})
        elif action == sqlite3.SQLITE_REINDEX:
            allowed = arg1 in allowed_indexes
        if allowed:
            return sqlite3.SQLITE_OK
        denials.append((action, arg1, arg2))
        return sqlite3.SQLITE_DENY

    return authorize


def _probe_pipeline_ledger_contract(connection: sqlite3.Connection) -> dict[str, bool]:
    results: dict[str, bool] = {}
    hashes = tuple(character * 64 for character in "01234567")
    connection.execute("SAVEPOINT pipeline_schema_62_probe")
    try:
        connection.execute(
            """
            INSERT INTO pipeline_coherency_dispositions (
                disposition_id, request_id, request_hash,
                candidate_instance_id, subject_key, trade_date, order_plan_id,
                sequence_no, action, reason_code, operator_id,
                expected_pipeline_fingerprint, expected_subject_version,
                expected_source_fingerprint, expected_candidate_fingerprint,
                expected_downstream_fingerprint, expected_boundary_fingerprint,
                evidence_type, evidence_ref, evidence_sha256, created_at
            ) VALUES (
                'apply-probe-one', 'apply-probe-request-one', ?,
                'apply-probe-candidate', 'candidate:apply-probe-candidate',
                '2026-01-01', 'apply-probe-plan', 1,
                'DISPOSE_EXPIRED_PLAN_READY', 'APPLY_PROBE', 'apply.probe',
                ?, ?, ?, ?, ?, ?, 'APPLY_PROBE', 'apply-probe-evidence', ?,
                '2026-01-01T00:00:00Z'
            )
            """,
            hashes,
        )
        results["valid_insert"] = True
        results["valid_revoke"] = _probe_success(
            connection,
            """
            INSERT INTO pipeline_coherency_dispositions (
                disposition_id, request_id, request_hash,
                candidate_instance_id, subject_key, sequence_no, action,
                supersedes_disposition_id, reason_code, operator_id,
                expected_pipeline_fingerprint, expected_subject_version,
                expected_source_fingerprint, expected_candidate_fingerprint,
                expected_downstream_fingerprint, expected_boundary_fingerprint,
                evidence_type, evidence_ref, evidence_sha256, created_at
            ) VALUES (
                'apply-probe-revoke', 'apply-probe-request-revoke', ?,
                'apply-probe-candidate', 'candidate:apply-probe-candidate', 2,
                'REVOKE', 'apply-probe-one', 'APPLY_PROBE_REVOKE', 'apply.probe',
                ?, ?, ?, ?, ?, ?, 'APPLY_PROBE', 'apply-probe-revoke', ?,
                '2026-01-01T00:00:01Z'
            )
            """,
            tuple(character * 64 for character in "89abcdef"),
        )
        invalid_sql = """
            INSERT INTO pipeline_coherency_dispositions (
                disposition_id, request_id, request_hash,
                candidate_instance_id, subject_key, sequence_no, action,
                reason_code, operator_id, expected_pipeline_fingerprint,
                expected_subject_version, expected_source_fingerprint,
                expected_candidate_fingerprint,
                expected_downstream_fingerprint,
                expected_boundary_fingerprint, evidence_type, evidence_ref,
                evidence_sha256, created_at{extra_columns}
            ) VALUES (?, ?, ?, 'apply-probe-candidate',
                'candidate:apply-probe-candidate', ?, ?, 'APPLY_PROBE',
                'apply.probe', ?, ?, ?, ?, ?, ?, 'APPLY_PROBE',
                'apply-probe-invalid', ?, '2026-01-01T00:00:02Z'{extra_values})
        """
        base = (
            "apply-probe-invalid",
            "apply-probe-invalid-request",
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
        cases = {
            "request_unique": (
                ("apply-probe-request-duplicate", "apply-probe-request-one", *base[2:]),
                "",
                "",
            ),
            "sequence_unique": (
                (
                    "apply-probe-sequence-duplicate",
                    "apply-probe-sequence-request",
                    "0" * 64,
                    1,
                    *base[4:],
                ),
                "",
                "",
            ),
            "action_check": (
                (
                    "apply-probe-action-invalid",
                    "apply-probe-action-request",
                    "0" * 64,
                    3,
                    "INVALID",
                    *base[5:],
                ),
                "",
                "",
            ),
            "sequence_positive": (
                (
                    "apply-probe-sequence-zero",
                    "apply-probe-sequence-zero-request",
                    "0" * 64,
                    0,
                    *base[4:],
                ),
                "",
                "",
            ),
            "lower_hex_hash": (
                (
                    "apply-probe-hash-invalid",
                    "apply-probe-hash-request",
                    "A" * 64,
                    *base[3:],
                ),
                "",
                "",
            ),
            "supersedes_foreign_key": (
                (
                    "apply-probe-fk-invalid",
                    "apply-probe-fk-request",
                    *base[2:],
                ),
                ", supersedes_disposition_id",
                ", 'apply-probe-missing'",
            ),
        }
        for name, (params, extra_columns, extra_values) in cases.items():
            results[name] = _probe_integrity_error(
                connection,
                invalid_sql.format(
                    extra_columns=extra_columns,
                    extra_values=extra_values,
                ),
                params,
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
            results[f"{column}_check"] = _probe_integrity_error(
                connection,
                invalid_sql.format(
                    extra_columns=f", {column}",
                    extra_values=", ?",
                ),
                (*base, invalid_value),
            )
        results["update_blocked"] = _probe_integrity_error(
            connection,
            """
            UPDATE pipeline_coherency_dispositions
            SET reason_code = 'MUTATED'
            WHERE disposition_id = 'apply-probe-one'
            """,
        )
        results["delete_blocked"] = _probe_integrity_error(
            connection,
            """
            DELETE FROM pipeline_coherency_dispositions
            WHERE disposition_id = 'apply-probe-one'
            """,
        )
    finally:
        connection.execute("ROLLBACK TO pipeline_schema_62_probe")
        connection.execute("RELEASE pipeline_schema_62_probe")
    results["ledger_empty_after_probe"] = (
        int(
            connection.execute("SELECT COUNT(*) FROM pipeline_coherency_dispositions").fetchone()[0]
        )
        == 0
    )
    return results


def _assert_target_snapshot(
    snapshot: Mapping[str, Any], *, source_snapshot: Mapping[str, Any]
) -> None:
    if snapshot.get("schema_version") != TARGET_SCHEMA:
        raise MigrationApplyError("TARGET_SCHEMA_MISMATCH")
    source_tables = _mapping(source_snapshot.get("table_content"))
    target_tables = _mapping(snapshot.get("table_content"))
    if set(target_tables) != {*source_tables, PIPELINE_QUALIFICATION_TABLE}:
        raise MigrationApplyError("TARGET_TABLE_SET_MISMATCH")
    if any(target_tables.get(name) != value for name, value in source_tables.items()):
        raise MigrationApplyError("EXISTING_TABLE_CONTENT_CHANGED")
    if snapshot.get("sqlite_sequence") != source_snapshot.get("sqlite_sequence"):
        raise MigrationApplyError("SQLITE_SEQUENCE_CHANGED")
    if snapshot.get("projection_outbox") != source_snapshot.get("projection_outbox"):
        raise MigrationApplyError("PROJECTION_OUTBOX_CHANGED")
    ledger = _mapping(target_tables.get(PIPELINE_QUALIFICATION_TABLE))
    if int(ledger.get("row_count") or 0) != 0:
        raise MigrationApplyError("TARGET_LEDGER_NOT_EMPTY")
    required_tables = _mapping(snapshot.get("required_tables"))
    required_columns = _mapping(snapshot.get("required_columns"))
    required_indexes = _mapping(snapshot.get("required_indexes"))
    required_triggers = _mapping(snapshot.get("required_triggers"))
    if not all(required_tables.get(name) is True for name in REQUIRED_TARGET_TABLES):
        raise MigrationApplyError("TARGET_REQUIRED_TABLE_MISSING")
    if not all(
        _mapping(required_columns.get(name)).get("valid") is True
        for name in REQUIRED_TARGET_COLUMNS
    ):
        raise MigrationApplyError("TARGET_COLUMN_CONTRACT_INVALID")
    if not all(
        _mapping(required_indexes.get(name)).get("valid") is True
        for name in REQUIRED_TARGET_INDEX_CONTRACTS
    ):
        raise MigrationApplyError("TARGET_INDEX_CONTRACT_INVALID")
    if not all(
        _mapping(required_triggers.get(name)).get("valid") is True
        for name in REQUIRED_TARGET_TRIGGER_CONTRACTS
    ):
        raise MigrationApplyError("TARGET_TRIGGER_CONTRACT_INVALID")


def _strict_read_only_snapshot(path: Path) -> tuple[list[str], dict[str, Any]]:
    _assert_no_sidecars(path, code="READ_ONLY_SNAPSHOT_SIDECAR_PRESENT")
    connection = _open_read_only(path)
    try:
        quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
        snapshot = _database_snapshot(connection)
    finally:
        connection.close()
    return quick_check, snapshot


def _exclusive_postcommit_snapshot(
    path: Path,
) -> tuple[list[str], dict[str, Any], dict[str, Any], dict[str, int]]:
    _assert_no_sidecars(path, code="EXCLUSIVE_POSTCHECK_SIDECAR_PRESENT")
    connection = _open_read_write(path)
    try:
        connection.execute("BEGIN EXCLUSIVE")
        connection.execute("PRAGMA query_only=ON")
        quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
        snapshot = _database_snapshot(connection)
        lease_counts = _lease_counts(connection)
        main_fingerprint = _main_fingerprint(path)
        _assert_single_link(path, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
    finally:
        try:
            if connection.in_transaction:
                connection.rollback()
        finally:
            connection.close()
    return quick_check, snapshot, main_fingerprint, lease_counts


def _reconcile_apply_state(
    source: Path,
    *,
    expected_source_files: Mapping[str, Any],
    expected_source_snapshot: Mapping[str, Any],
) -> str:
    try:
        _assert_no_sidecars(source, code="RECONCILE_SIDECAR_PRESENT")
        _assert_single_link(source, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
        quick_check, snapshot = _strict_read_only_snapshot(source)
        if quick_check != ["ok"]:
            return "AMBIGUOUS"
        current_files = _file_fingerprints(source)
        _assert_no_sidecars(source, code="RECONCILE_SIDECAR_APPEARED")
        _assert_single_link(source, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
        if snapshot == expected_source_snapshot and current_files == expected_source_files:
            return "SOURCE_61_UNCHANGED"

        _assert_target_snapshot(snapshot, source_snapshot=expected_source_snapshot)
        (
            locked_quick_check,
            locked_snapshot,
            locked_main,
            locked_leases,
        ) = _exclusive_postcommit_snapshot(source)
        if locked_quick_check != ["ok"]:
            return "AMBIGUOUS"
        _assert_zero_leases(locked_leases)
        _assert_target_snapshot(
            locked_snapshot,
            source_snapshot=expected_source_snapshot,
        )
        if not _logical_snapshots_equal(snapshot, locked_snapshot):
            return "AMBIGUOUS"
        final_files = _file_fingerprints(source)
        _assert_no_sidecars(source, code="RECONCILE_FINAL_SIDECAR_PRESENT")
        _assert_single_link(source, code="SOURCE_HARDLINK_COUNT_NOT_ONE")
        if _mapping(final_files.get("main")) != locked_main:
            return "AMBIGUOUS"
        return "TARGET_62_APPLIED"
    except Exception:
        return "AMBIGUOUS"


def _lease_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    counts = {
        "runtime_execution_locks": 0,
        "projection_outbox_processing_or_locked": 0,
        "live_sim_lifecycle_inbox_processing_or_locked": 0,
    }
    if "runtime_execution_locks" in tables:
        counts["runtime_execution_locks"] = int(
            connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0]
        )
    if "projection_outbox" in tables:
        counts["projection_outbox_processing_or_locked"] = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM projection_outbox
                WHERE status = 'PROCESSING'
                   OR locked_by IS NOT NULL
                   OR locked_at IS NOT NULL
                """
            ).fetchone()[0]
        )
    if "live_sim_lifecycle_inbox" in tables:
        counts["live_sim_lifecycle_inbox_processing_or_locked"] = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM live_sim_lifecycle_inbox
                WHERE status = 'PROCESSING'
                   OR locked_by IS NOT NULL
                   OR locked_at IS NOT NULL
                """
            ).fetchone()[0]
        )
    return counts


def _assert_zero_leases(counts: Mapping[str, int]) -> None:
    if any(int(value) != 0 for value in counts.values()):
        raise MigrationApplyError("ACTIVE_WRITER_OR_LEASE_PRESENT")


def _copy_byte_identical(*, source: Path, backup: Path) -> None:
    try:
        with source.open("rb") as source_stream, backup.open("xb") as backup_stream:
            shutil.copyfileobj(source_stream, backup_stream, length=8 * 1024 * 1024)
            backup_stream.flush()
            os.fsync(backup_stream.fileno())
        shutil.copystat(source, backup)
    except FileExistsError as exc:
        raise MigrationApplyError("BACKUP_ARTIFACT_ALREADY_EXISTS") from exc


def _disk_contract(*, source: Path, backup: Path) -> dict[str, int | bool]:
    probe = backup.parent
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    source_bytes = int(source.stat().st_size)
    required_bytes = max(source_bytes * 2, source_bytes + MIN_FREE_MARGIN_BYTES)
    free_bytes = int(shutil.disk_usage(probe).free)
    return {
        "source_bytes": source_bytes,
        "free_bytes": free_bytes,
        "required_bytes": required_bytes,
        "sufficient": free_bytes >= required_bytes,
    }


def _assert_single_link(path: Path, *, code: str) -> None:
    if int(path.stat().st_nlink) != 1:
        raise MigrationApplyError(code)


def _assert_no_sidecars(path: Path, *, code: str) -> None:
    if _sidecar_count(path):
        raise MigrationApplyError(code)


def _sidecar_count(path: Path) -> int:
    return sum(Path(f"{path}{suffix}").exists() for suffix in SIDECAR_SUFFIXES)


def _main_fingerprint(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "exists": True,
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": _sha256_file(path),
    }


def _public_fingerprint(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "size": int(value.get("size") or 0),
        "mtime_ns": value.get("mtime_ns"),
        "sha256": value.get("sha256"),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _probe_integrity_error(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[object, ...] = (),
) -> bool:
    try:
        connection.execute(sql, params)
    except sqlite3.IntegrityError:
        return True
    return False


def _probe_success(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[object, ...] = (),
) -> bool:
    try:
        connection.execute(sql, params)
    except sqlite3.Error:
        return False
    return True


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_INVALID")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_INVALID") from exc
    if parsed.tzinfo is None:
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_INVALID")
    return parsed.astimezone(UTC)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _logical_snapshots_equal(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return {key: value for key, value in left.items() if key != "journal_mode"} == {
        key: value for key, value in right.items() if key != "journal_mode"
    }


def _emit_failure(
    exc: MigrationApplyError,
    *,
    source_db: Path,
    backup_db: Path,
    preflight_raw: Path,
    out_dir: Path,
) -> None:
    commit_state = "APPLIED_OR_UNKNOWN_FAIL_CLOSED" if exc.committed else "NOT_APPLIED_VERIFIED"
    print(
        f"database migration apply: FAIL code={exc.code} "
        f"committed={str(exc.committed).lower()} "
        f"commit_state={commit_state} evidence=pending",
        flush=True,
    )
    failure = _failure_report(
        exc,
        source_db=source_db,
        backup_db=backup_db,
        preflight_raw=preflight_raw,
    )
    try:
        paths = _write_report(failure, out_dir=out_dir)
    except Exception as evidence_exc:
        print(
            "database migration apply: failure_evidence=UNAVAILABLE "
            f"error_type={type(evidence_exc).__name__}",
            flush=True,
        )
        return
    print(
        f"database migration apply: failure_evidence={paths['raw_json']}",
        flush=True,
    )


def _failure_report(
    exc: MigrationApplyError,
    *,
    source_db: Path,
    backup_db: Path,
    preflight_raw: Path,
) -> dict[str, Any]:
    return {
        "generated_at": _now(),
        "status": "FAIL",
        "committed": exc.committed,
        "commit_state": (
            "APPLIED_OR_UNKNOWN_FAIL_CLOSED" if exc.committed else "NOT_APPLIED_VERIFIED"
        ),
        "error_code": exc.code,
        "source_path": str(source_db.expanduser().resolve()),
        "backup_path": str(backup_db.expanduser().resolve()),
        "preflight_raw_path": str(preflight_raw.expanduser().resolve()),
        "source_schema_required": SOURCE_SCHEMA,
        "target_schema": TARGET_SCHEMA,
        "raw_rows_included": False,
        "secrets_included": False,
    }


def _write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    report_dir = out_dir.expanduser().resolve() / stamp
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(
        "\n".join(
            (
                "# Database Migration Apply",
                "",
                f"- status: `{report.get('status', 'UNKNOWN')}`",
                f"- committed: `{str(bool(report.get('committed'))).lower()}`",
                f"- schema: `{SOURCE_SCHEMA} -> {TARGET_SCHEMA}`",
                f"- error_code: `{report.get('error_code', '')}`",
                "- evidence_scope: `hashes/counts only; no raw rows or secrets`",
                "",
            )
        ),
        encoding="utf-8",
    )
    return {"raw_json": raw_path, "summary_md": summary_path}


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
