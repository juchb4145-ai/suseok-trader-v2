from __future__ import annotations

import argparse
import hashlib
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
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_INDEXES,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE,
    GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TRIGGERS,
    SCHEMA_VERSION,
    migrate_schema_62_to_63,
)
from tools import ops_database_migration_62_to_63_preflight as preflight_tool  # noqa: E402
from tools import ops_database_migration_apply as historical_apply  # noqa: E402

MigrationApplyError = historical_apply.MigrationApplyError

SOURCE_SCHEMA = preflight_tool.SOURCE_SCHEMA
TARGET_SCHEMA = preflight_tool.TARGET_SCHEMA
APPLY_ACKNOWLEDGEMENT = "APPLY_EXACT_SCHEMA_62_TO_63_FENCE_EVENT_LEDGER"
MAX_PREFLIGHT_AGE_SEC = 3_600
MAX_FUTURE_SKEW_SEC = 300
SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
FENCE_EVENT_SAVEPOINT = "gateway_order_boundary_fence_event_migration"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Apply only a fresh, PASS-qualified exact SQLite schema 62 -> 63 "
            "append-only fence-event migration."
        )
    )
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--preflight-raw", required=True)
    parser.add_argument("--preflight-raw-sha256", required=True)
    parser.add_argument("--backup-db", required=True)
    parser.add_argument("--acknowledge", required=True)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "database_migration_62_to_63_apply"),
    )
    args = parser.parse_args()

    try:
        report = run_apply(
            source_db=Path(args.source_db),
            preflight_raw=Path(args.preflight_raw),
            expected_preflight_raw_sha256=args.preflight_raw_sha256,
            backup_db=Path(args.backup_db),
            acknowledge=args.acknowledge,
            out_dir=Path(args.out_dir),
        )
    except MigrationApplyError as exc:
        print(
            "database migration 62->63 apply: FAIL "
            f"code={exc.code} committed={str(exc.committed).lower()}",
            flush=True,
        )
        return 2

    print(
        "database migration 62->63 apply: PASS "
        f"backup_sha256={_mapping(report.get('backup')).get('sha256')} "
        f"raw={_mapping(report.get('report_paths')).get('raw_json')}",
        flush=True,
    )
    return 0


def run_apply(
    *,
    source_db: Path,
    preflight_raw: Path,
    expected_preflight_raw_sha256: str,
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
    if not _SHA256_RE.fullmatch(str(expected_preflight_raw_sha256)):
        raise MigrationApplyError("PREFLIGHT_RAW_SHA256_INVALID")
    if str(SCHEMA_VERSION) != TARGET_SCHEMA:
        raise MigrationApplyError("CURRENT_CODE_TARGET_SCHEMA_MISMATCH")
    _validate_paths(source=source, preflight_raw=preflight_path, backup=backup)

    preflight_bytes = preflight_path.read_bytes()
    preflight_sha256 = hashlib.sha256(preflight_bytes).hexdigest()
    if preflight_sha256 != expected_preflight_raw_sha256:
        raise MigrationApplyError("PREFLIGHT_RAW_SHA256_MISMATCH")
    try:
        loaded = json.loads(preflight_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationApplyError("PREFLIGHT_RAW_INVALID") from exc
    preflight = _mapping(loaded)
    preflight_age_sec = _validate_preflight(preflight, source=source)
    source_report = _mapping(preflight.get("source"))
    expected_source_snapshot = _mapping(source_report.get("snapshot"))
    expected_source_files = _mapping(source_report.get("files_after"))

    historical_apply._assert_no_sidecars(source, code="SOURCE_SIDECAR_PRESENT")
    historical_apply._assert_single_link(
        source,
        code="SOURCE_HARDLINK_COUNT_NOT_ONE",
    )
    if preflight_tool.snapshot_tool._file_fingerprints(source) != expected_source_files:
        raise MigrationApplyError("SOURCE_FINGERPRINT_CHANGED_SINCE_PREFLIGHT")
    disk = historical_apply._disk_contract(source=source, backup=backup)
    if disk.get("sufficient") is not True:
        raise MigrationApplyError("BACKUP_DISK_SPACE_INSUFFICIENT")

    read_only_started = time.perf_counter()
    source_quick_check, fresh_source_snapshot = _strict_read_only_snapshot(source)
    if source_quick_check != ["ok"]:
        raise MigrationApplyError("SOURCE_QUICK_CHECK_FAILED")
    if fresh_source_snapshot != expected_source_snapshot:
        raise MigrationApplyError("SOURCE_TYPED_SNAPSHOT_CHANGED_SINCE_PREFLIGHT")
    source_ro = preflight_tool.snapshot_tool._open_read_only(source)
    try:
        lease_counts = historical_apply._lease_counts(source_ro)
    finally:
        source_ro.close()
    historical_apply._assert_zero_leases(lease_counts)
    read_only_elapsed_sec = time.perf_counter() - read_only_started
    if preflight_tool.snapshot_tool._file_fingerprints(source) != expected_source_files:
        raise MigrationApplyError("SOURCE_CHANGED_DURING_READ_ONLY_RECHECK")
    historical_apply._assert_no_sidecars(
        source,
        code="SOURCE_SIDECAR_APPEARED_DURING_RECHECK",
    )

    backup.parent.mkdir(parents=True, exist_ok=True)
    connection = historical_apply._open_read_write(source)
    apply_error: MigrationApplyError | None = None
    authorizer_denials: list[tuple[int, str | None, str | None]] = []
    precommit_snapshot: dict[str, Any] = {}
    backup_snapshot: dict[str, Any] = {}
    backup_quick_check: list[str] = []
    backup_fingerprint: dict[str, Any] = {}
    exclusive_leases: dict[str, int] = {}
    try:
        connection.execute("BEGIN EXCLUSIVE")
        exclusive_leases = historical_apply._lease_counts(connection)
        historical_apply._assert_zero_leases(exclusive_leases)
        if historical_apply._main_fingerprint(source) != _mapping(
            expected_source_files.get("main")
        ):
            raise MigrationApplyError("SOURCE_CHANGED_BEFORE_EXCLUSIVE_APPLY")

        historical_apply._copy_byte_identical(source=source, backup=backup)
        historical_apply._assert_single_link(
            backup,
            code="BACKUP_HARDLINK_COUNT_NOT_ONE",
        )
        backup_fingerprint = historical_apply._main_fingerprint(backup)
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
        historical_apply._assert_no_sidecars(
            backup,
            code="BACKUP_SIDECAR_PRESENT",
        )

        connection.set_authorizer(_migration_authorizer(authorizer_denials))
        try:
            migrate_schema_62_to_63(connection)
        except sqlite3.DatabaseError as exc:
            raise MigrationApplyError("MIGRATION_AUTHORIZER_OR_SQL_FAILURE") from exc
        finally:
            connection.set_authorizer(None)
        if authorizer_denials:
            raise MigrationApplyError("MIGRATION_AUTHORIZER_DENIED_OPERATION")

        precommit_quick_check = _quick_check_connection(connection)
        if precommit_quick_check != ["ok"]:
            raise MigrationApplyError("PRECOMMIT_QUICK_CHECK_FAILED")
        precommit_snapshot = preflight_tool._database_snapshot(connection)
        _assert_target_snapshot(
            precommit_snapshot,
            source_snapshot=expected_source_snapshot,
        )
        if historical_apply._main_fingerprint(backup) != backup_fingerprint:
            raise MigrationApplyError("BACKUP_CHANGED_BEFORE_COMMIT")
        connection.commit()
    except MigrationApplyError as exc:
        apply_error = exc
        try:
            if connection.in_transaction:
                connection.rollback()
        except Exception:
            apply_error = MigrationApplyError("ROLLBACK_FAILED")
    except Exception as exc:
        apply_error = MigrationApplyError("UNEXPECTED_PRECOMMIT_FAILURE")
        apply_error.__cause__ = exc
        try:
            if connection.in_transaction:
                connection.rollback()
        except Exception:
            apply_error = MigrationApplyError("ROLLBACK_FAILED")
    finally:
        try:
            connection.set_authorizer(None)
        except Exception:
            if apply_error is None:
                apply_error = MigrationApplyError("AUTHORIZER_CLEANUP_FAILED")
        try:
            connection.close()
        except Exception:
            if apply_error is None:
                apply_error = MigrationApplyError("CONNECTION_CLOSE_FAILED")

    if apply_error is not None:
        state = _reconcile_apply_state(
            source,
            expected_source_files=expected_source_files,
            expected_source_snapshot=expected_source_snapshot,
        )
        if state == "SOURCE_62_UNCHANGED":
            raise apply_error
        if state == "TARGET_63_APPLIED":
            raise MigrationApplyError(apply_error.code, committed=True) from apply_error
        raise MigrationApplyError("MIGRATION_STATE_AMBIGUOUS", committed=True) from apply_error

    try:
        historical_apply._assert_no_sidecars(
            source,
            code="SOURCE_SIDECAR_LEFT_AFTER_COMMIT",
        )
        historical_apply._assert_no_sidecars(
            backup,
            code="BACKUP_SIDECAR_LEFT_AFTER_COMMIT",
        )
        post_quick_check, post_snapshot = _strict_read_only_snapshot(source)
        if post_quick_check != ["ok"]:
            raise MigrationApplyError("POST_COMMIT_QUICK_CHECK_FAILED", committed=True)
        _assert_target_snapshot(post_snapshot, source_snapshot=expected_source_snapshot)
        if not _logical_snapshots_equal(post_snapshot, precommit_snapshot):
            raise MigrationApplyError("POST_COMMIT_SNAPSHOT_MISMATCH", committed=True)

        (
            locked_quick_check,
            locked_snapshot,
            locked_main,
            locked_leases,
        ) = _exclusive_postcommit_snapshot(source)
        if locked_quick_check != ["ok"]:
            raise MigrationApplyError(
                "EXCLUSIVE_POST_COMMIT_QUICK_CHECK_FAILED",
                committed=True,
            )
        historical_apply._assert_zero_leases(locked_leases)
        _assert_target_snapshot(locked_snapshot, source_snapshot=expected_source_snapshot)
        if not _logical_snapshots_equal(locked_snapshot, post_snapshot):
            raise MigrationApplyError(
                "EXCLUSIVE_POST_COMMIT_SNAPSHOT_MISMATCH",
                committed=True,
            )
        final_files = preflight_tool.snapshot_tool._file_fingerprints(source)
        historical_apply._assert_no_sidecars(
            source,
            code="SOURCE_SIDECAR_LEFT_AFTER_FINAL_CHECK",
        )
        if _mapping(final_files.get("main")) != locked_main:
            raise MigrationApplyError(
                "SOURCE_FINGERPRINT_CHANGED_AFTER_FINAL_SNAPSHOT",
                committed=True,
            )
        if historical_apply._main_fingerprint(backup) != backup_fingerprint:
            raise MigrationApplyError("BACKUP_CHANGED_AFTER_COMMIT", committed=True)
    except MigrationApplyError as exc:
        if not exc.committed:
            raise MigrationApplyError(exc.code, committed=True) from exc
        raise
    except Exception as exc:
        raise MigrationApplyError("UNEXPECTED_POSTCOMMIT_FAILURE", committed=True) from exc

    try:
        backup_stat = backup.stat()
        report: dict[str, Any] = {
            "generated_at": _now(),
            "contract": "exact-schema-62-to-63-apply.v1",
            "status": "PASS",
            "committed": True,
            "migration": {
                "source_schema": SOURCE_SCHEMA,
                "target_schema": TARGET_SCHEMA,
                "exact_function": "storage.sqlite.migrate_schema_62_to_63",
                "acknowledgement_matched": True,
                "authorizer_denial_count": len(authorizer_denials),
            },
            "source": {
                "path": str(source),
                "schema_before": SOURCE_SCHEMA,
                "schema_after": str(post_snapshot.get("schema_version")),
                "preflight_main": historical_apply._public_fingerprint(
                    _mapping(expected_source_files.get("main"))
                ),
                "final_main": historical_apply._public_fingerprint(
                    _mapping(final_files.get("main"))
                ),
                "post_main": historical_apply._public_fingerprint(
                    _mapping(final_files.get("main"))
                ),
                "exclusive_post_commit_main": historical_apply._public_fingerprint(locked_main),
                "hardlink_count": int(source.stat().st_nlink),
                "strict_read_only_quick_check": source_quick_check,
                "post_commit_quick_check": post_quick_check,
                "exclusive_post_commit_quick_check": locked_quick_check,
                "sidecar_count_after": historical_apply._sidecar_count(source),
                "strict_read_only_elapsed_sec": round(read_only_elapsed_sec, 6),
            },
            "backup": {
                "path": str(backup),
                "size": int(backup_stat.st_size),
                "sha256": str(backup_fingerprint.get("sha256")),
                "byte_identical": True,
                "schema_version": str(backup_snapshot.get("schema_version")),
                "quick_check": backup_quick_check,
                "sidecar_count": historical_apply._sidecar_count(backup),
                "hardlink_count": int(backup_stat.st_nlink),
            },
            "preflight": {
                "path": str(preflight_path),
                "sha256": preflight_sha256,
                "expected_sha256": expected_preflight_raw_sha256,
                "approval_digest_matched": True,
                "generated_at": str(preflight.get("generated_at")),
                "age_sec": round(preflight_age_sec, 6),
                "verdict": "PASS",
            },
            "safety": {
                "disk": disk,
                "lease_counts": lease_counts,
                "exclusive_lease_counts": exclusive_leases,
                "exclusive_post_commit_lease_counts": locked_leases,
                "source_preflight_fingerprint_matched": True,
                "backup_snapshot_matched": True,
                "existing_table_content_preserved": True,
                "order_state_preserved": True,
                "projection_outbox_preserved": True,
                "sqlite_sequence_preserved": True,
                "target_ledger_empty": True,
                "target_contract_valid": True,
                "precommit_postcommit_snapshot_equal": _logical_snapshots_equal(
                    post_snapshot,
                    precommit_snapshot,
                ),
                "readonly_exclusive_snapshot_equal": _logical_snapshots_equal(
                    locked_snapshot,
                    post_snapshot,
                ),
                "exclusive_final_main_fingerprint_equal": (
                    _mapping(final_files.get("main")) == locked_main
                ),
                "exact_precommit_postcommit_snapshot_equal": _logical_snapshots_equal(
                    post_snapshot,
                    precommit_snapshot,
                ),
                "postcommit_readonly_exclusive_snapshot_equal": _logical_snapshots_equal(
                    locked_snapshot,
                    post_snapshot,
                ),
                "exclusive_postcommit_final_fingerprint_equal": (
                    _mapping(final_files.get("main")) == locked_main
                ),
                "exclusive_transaction_acquired": True,
                "exclusive_post_commit_transaction_acquired": True,
                "exclusive_post_commit_runtime_lease_count": int(
                    locked_leases.get("runtime_execution_locks", 0)
                ),
                "source_sidecar_count_before": 0,
            },
            "counts": {
                "existing_table_count": len(
                    _mapping(expected_source_snapshot.get("table_content"))
                ),
                "target_ledger_row_count": 0,
            },
            "evidence_policy": {
                "hashes_and_counts_only": True,
                "raw_rows_included": False,
                "secrets_included": False,
                "account_numbers_included": False,
            },
            "elapsed_sec": round(time.perf_counter() - started, 6),
        }
    except MigrationApplyError as exc:
        if not exc.committed:
            raise MigrationApplyError(exc.code, committed=True) from exc
        raise
    except Exception as exc:
        raise MigrationApplyError("EVIDENCE_BUILD_FAILED", committed=True) from exc

    try:
        paths = _write_report(report, out_dir=out_dir)
        report["report_paths"] = {name: str(path) for name, path in paths.items()}
    except MigrationApplyError as exc:
        if not exc.committed:
            raise MigrationApplyError(exc.code, committed=True) from exc
        raise
    except Exception as exc:
        raise MigrationApplyError("EVIDENCE_WRITE_FAILED", committed=True) from exc
    return report


def _validate_preflight(report: Mapping[str, Any], *, source: Path) -> float:
    verdict = _mapping(report.get("verdict"))
    recomputed = preflight_tool.evaluate_report(report)
    source_report = _mapping(report.get("source"))
    source_snapshot = _mapping(source_report.get("snapshot"))
    clone = _mapping(report.get("clone"))
    after = _mapping(clone.get("after_exact_migration"))
    required_true = (
        verdict.get("source_files_unchanged"),
        verdict.get("backup_tables_preserved"),
        verdict.get("target_table_set_exact"),
        verdict.get("existing_table_content_preserved"),
        verdict.get("order_state_preserved"),
        verdict.get("projection_outbox_preserved"),
        verdict.get("sqlite_sequence_preserved"),
        verdict.get("target_objects_absent_at_source"),
        verdict.get("target_objects_exact"),
        verdict.get("target_contract_valid"),
        verdict.get("target_ledger_empty"),
        verdict.get("initializer_noop"),
        verdict.get("quick_checks_ok"),
    )
    if (
        verdict.get("status") != "PASS"
        or verdict != recomputed
        or verdict.get("failures") != []
        or not all(value is True for value in required_true)
        or verdict.get("source_original_write_detected") is not False
        or report.get("preflight_contract_version") != preflight_tool.PREFLIGHT_CONTRACT_VERSION
        or report.get("required_source_schema") != SOURCE_SCHEMA
        or report.get("target_schema_version") != TARGET_SCHEMA
        or source_snapshot.get("schema_version") != SOURCE_SCHEMA
        or after.get("schema_version") != TARGET_SCHEMA
        or clone.get("migration_method") != preflight_tool.EXACT_MIGRATION_METHOD
        or source_report.get("files_before") != source_report.get("files_after")
        or source_report.get("quick_check") != ["ok"]
        or clone.get("before_quick_check") != ["ok"]
        or clone.get("exact_quick_check") != ["ok"]
        or clone.get("quick_check") != ["ok"]
        or source_report.get("opened_read_only") is not True
        or source_report.get("query_only") is not True
        or source_report.get("immutable") is not True
    ):
        raise MigrationApplyError("PREFLIGHT_CONTRACT_NOT_PASS")
    if Path(str(source_report.get("path", ""))).expanduser().resolve() != source:
        raise MigrationApplyError("PREFLIGHT_SOURCE_PATH_MISMATCH")

    generated_at = _parse_timestamp(report.get("generated_at"))
    age_sec = (datetime.now(UTC) - generated_at).total_seconds()
    if age_sec < -MAX_FUTURE_SKEW_SEC:
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_IN_FUTURE")
    if age_sec > MAX_PREFLIGHT_AGE_SEC:
        raise MigrationApplyError("PREFLIGHT_STALE")
    return max(age_sec, 0.0)


def _assert_target_snapshot(
    snapshot: Mapping[str, Any],
    *,
    source_snapshot: Mapping[str, Any],
) -> None:
    if snapshot.get("schema_version") != TARGET_SCHEMA:
        raise MigrationApplyError("TARGET_SCHEMA_MISMATCH")
    if not preflight_tool._identity_matches(
        _mapping(snapshot.get("app_identity")),
        schema=TARGET_SCHEMA,
    ):
        raise MigrationApplyError("TARGET_APPLICATION_IDENTITY_INVALID")

    source_tables = _mapping(source_snapshot.get("table_content"))
    target_tables = _mapping(snapshot.get("table_content"))
    expected_tables = {*source_tables, GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE}
    if set(target_tables) != expected_tables:
        raise MigrationApplyError("TARGET_TABLE_SET_MISMATCH")
    if any(target_tables.get(name) != value for name, value in source_tables.items()):
        raise MigrationApplyError("EXISTING_TABLE_CONTENT_CHANGED")
    ledger = _mapping(target_tables.get(GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE))
    if ledger.get("row_count") != 0:
        raise MigrationApplyError("TARGET_LEDGER_NOT_EMPTY")
    if snapshot.get("projection_outbox") != source_snapshot.get("projection_outbox"):
        raise MigrationApplyError("PROJECTION_OUTBOX_CHANGED")
    if snapshot.get("sqlite_sequence") != source_snapshot.get("sqlite_sequence"):
        raise MigrationApplyError("SQLITE_SEQUENCE_CHANGED")
    if snapshot.get("order_state") != source_snapshot.get("order_state"):
        raise MigrationApplyError("ORDER_STATE_CHANGED")
    if _mapping(snapshot.get("migration_objects")) != preflight_tool.TARGET_OBJECT_TYPES:
        raise MigrationApplyError("TARGET_OBJECT_SET_INVALID")
    if _mapping(snapshot.get("fence_event_contract")).get("ready") is not True:
        raise MigrationApplyError("TARGET_FENCE_EVENT_CONTRACT_INVALID")


def _migration_authorizer(
    denials: list[tuple[int, str | None, str | None]],
):
    allowed_indexes = set(GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_INDEXES)
    allowed_triggers = set(GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TRIGGERS)
    autoindex_prefix = f"sqlite_autoindex_{GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE}_"
    read_actions = {sqlite3.SQLITE_READ, sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION}
    allowed_pragmas = {
        "database_list",
        "foreign_key_list",
        "table_info",
        "table_xinfo",
        "index_list",
        "index_xinfo",
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
            allowed = str(arg1 or "").lower() in allowed_pragmas
        elif action == sqlite3.SQLITE_CREATE_TABLE:
            allowed = arg1 == GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE
        elif action == sqlite3.SQLITE_CREATE_INDEX:
            allowed = bool(
                arg2 == GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE
                and arg1 is not None
                and (arg1 in allowed_indexes or arg1.startswith(autoindex_prefix))
            )
        elif action == sqlite3.SQLITE_CREATE_TRIGGER:
            allowed = bool(
                arg1 in allowed_triggers and arg2 == GATEWAY_ORDER_BROKER_BOUNDARY_FENCE_EVENT_TABLE
            )
        elif action == sqlite3.SQLITE_INSERT:
            allowed = arg1 == "sqlite_master"
        elif action == sqlite3.SQLITE_UPDATE:
            allowed = bool(
                (
                    arg1 == "sqlite_master"
                    and arg2 in {"type", "name", "tbl_name", "rootpage", "sql"}
                )
                or (arg1 == "app_metadata" and arg2 in {"value", "updated_at"})
            )
        elif action == sqlite3.SQLITE_REINDEX:
            allowed = arg1 in allowed_indexes
        elif action == sqlite3.SQLITE_SAVEPOINT:
            allowed = bool(
                str(arg1 or "").upper() in {"BEGIN", "RELEASE", "ROLLBACK"}
                and arg2 == FENCE_EVENT_SAVEPOINT
            )
        if allowed:
            return sqlite3.SQLITE_OK
        denials.append((action, arg1, arg2))
        return sqlite3.SQLITE_DENY

    return authorize


def _strict_read_only_snapshot(path: Path) -> tuple[list[str], dict[str, Any]]:
    historical_apply._assert_no_sidecars(
        path,
        code="READ_ONLY_SNAPSHOT_SIDECAR_PRESENT",
    )
    connection = preflight_tool.snapshot_tool._open_read_only(path)
    try:
        quick_check = _quick_check_connection(connection)
        snapshot = preflight_tool._database_snapshot(connection)
    finally:
        connection.close()
    return quick_check, snapshot


def _exclusive_postcommit_snapshot(
    path: Path,
) -> tuple[list[str], dict[str, Any], dict[str, Any], dict[str, int]]:
    historical_apply._assert_no_sidecars(
        path,
        code="EXCLUSIVE_POSTCHECK_SIDECAR_PRESENT",
    )
    connection = historical_apply._open_read_write(path)
    try:
        connection.execute("BEGIN EXCLUSIVE")
        connection.execute("PRAGMA query_only=ON")
        quick_check = _quick_check_connection(connection)
        snapshot = preflight_tool._database_snapshot(connection)
        leases = historical_apply._lease_counts(connection)
        main = historical_apply._main_fingerprint(path)
    finally:
        try:
            if connection.in_transaction:
                connection.rollback()
        finally:
            connection.close()
    return quick_check, snapshot, main, leases


def _reconcile_apply_state(
    source: Path,
    *,
    expected_source_files: Mapping[str, Any],
    expected_source_snapshot: Mapping[str, Any],
) -> str:
    try:
        historical_apply._assert_no_sidecars(source, code="RECONCILE_SIDECAR_PRESENT")
        quick_check, snapshot = _strict_read_only_snapshot(source)
        if quick_check != ["ok"]:
            return "AMBIGUOUS"
        current_files = preflight_tool.snapshot_tool._file_fingerprints(source)
        if snapshot == expected_source_snapshot and current_files == expected_source_files:
            return "SOURCE_62_UNCHANGED"
        _assert_target_snapshot(snapshot, source_snapshot=expected_source_snapshot)
        return "TARGET_63_APPLIED"
    except Exception:
        return "AMBIGUOUS"


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


def _quick_check_connection(connection: sqlite3.Connection) -> list[str]:
    return [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]


def _logical_snapshots_equal(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> bool:
    return {key: value for key, value in left.items() if key != "journal_mode"} == {
        key: value for key, value in right.items() if key != "journal_mode"
    }


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_INVALID")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_INVALID") from exc
    if parsed.tzinfo is None:
        raise MigrationApplyError("PREFLIGHT_TIMESTAMP_INVALID")
    return parsed.astimezone(UTC)


def _write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(
        "\n".join(
            (
                "# Exact SQLite 62→63 migration apply",
                "",
                "- status: `PASS`",
                "- committed: `true`",
                f"- source: `{_mapping(report.get('source')).get('path')}`",
                f"- backup SHA-256: `{_mapping(report.get('backup')).get('sha256')}`",
                "- existing/order state preserved: `true`",
                "- target fence-event ledger empty: `true`",
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
