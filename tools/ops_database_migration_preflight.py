from __future__ import annotations

import argparse
import json
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

from storage.sqlite import SCHEMA_VERSION, initialize_database  # noqa: E402

REQUIRED_TARGET_TABLES = (
    "market_scan_projection_routing_decisions",
    "market_scan_append_only_budget_state",
    "live_sim_lifecycle_inbox",
    "live_sim_lifecycle_consumer_runs",
    "live_sim_lifecycle_routing_decisions",
    "incremental_evaluation_dead_letters",
)
FINGERPRINT_FILES = {
    "main": "",
    "wal": "-wal",
    "shm": "-shm",
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Clone a SQLite operating database read-only and validate the current "
            "schema migration on the clone."
        )
    )
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--clone-db", required=True)
    parser.add_argument("--require-source-schema", default="")
    parser.add_argument("--skip-quick-check", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "database_migration_preflight"),
    )
    args = parser.parse_args()

    report = run_preflight(
        source_db=Path(args.source_db),
        clone_db=Path(args.clone_db),
        required_source_schema=args.require_source_schema or None,
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
    source_connection = _open_read_only(source)
    try:
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
    migrated = initialize_database(clone)
    migrated.close()
    migration_elapsed_sec = time.perf_counter() - migration_started

    idempotent_started = time.perf_counter()
    idempotent = initialize_database(clone)
    idempotent.close()
    idempotent_elapsed_sec = time.perf_counter() - idempotent_started

    clone_after = _snapshot_path(clone)
    quick_check_started = time.perf_counter()
    quick_check = _quick_check(clone) if run_quick_check else ["SKIPPED"]
    quick_check_elapsed_sec = time.perf_counter() - quick_check_started

    report: dict[str, Any] = {
        "generated_at": _now(),
        "source": {
            "path": str(source),
            "files_before": source_files_before,
            "files_after": source_files_after,
            "snapshot": source_snapshot,
            "opened_read_only": True,
            "query_only": True,
        },
        "clone": {
            "path": str(clone),
            "backup_elapsed_sec": backup_elapsed_sec,
            "migration_elapsed_sec": migration_elapsed_sec,
            "idempotent_elapsed_sec": idempotent_elapsed_sec,
            "quick_check_elapsed_sec": quick_check_elapsed_sec,
            "before_migration": clone_before,
            "after_migration": clone_after,
            "quick_check": quick_check,
        },
        "target_schema_version": str(SCHEMA_VERSION),
        "required_source_schema": required_source_schema,
    }
    report["verdict"] = evaluate_report(report)
    report_paths = _write_report(report, out_dir=out_dir)
    report["report_paths"] = {name: str(path) for name, path in report_paths.items()}
    return report


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(report.get("source"))
    clone = _mapping(report.get("clone"))
    source_snapshot = _mapping(source.get("snapshot"))
    clone_before = _mapping(clone.get("before_migration"))
    clone_after = _mapping(clone.get("after_migration"))
    failures: list[str] = []

    required_source_schema = report.get("required_source_schema")
    if required_source_schema and source_snapshot.get("schema_version") != str(
        required_source_schema
    ):
        failures.append("SOURCE_SCHEMA_MISMATCH")
    if clone_before.get("schema_version") != source_snapshot.get("schema_version"):
        failures.append("BACKUP_SCHEMA_MISMATCH")
    if clone_after.get("schema_version") != str(report.get("target_schema_version")):
        failures.append("TARGET_SCHEMA_MISMATCH")
    if clone_before.get("projection_outbox") != source_snapshot.get("projection_outbox"):
        failures.append("BACKUP_OUTBOX_MISMATCH")
    if clone_after.get("projection_outbox") != source_snapshot.get("projection_outbox"):
        failures.append("MIGRATION_OUTBOX_MISMATCH")

    required_tables = _mapping(clone_after.get("required_tables"))
    if not all(bool(required_tables.get(table)) for table in REQUIRED_TARGET_TABLES):
        failures.append("TARGET_TABLE_MISSING")
    if clone.get("quick_check") not in (["ok"], ["SKIPPED"]):
        failures.append("CLONE_QUICK_CHECK_FAILED")

    before_files = _mapping(source.get("files_before"))
    after_files = _mapping(source.get("files_after"))
    changed_source_files = _changed_source_data_files(
        before=before_files,
        after=after_files,
    )
    if changed_source_files:
        failures.append("SOURCE_DATA_FILE_CHANGED")

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
        "required_tables_present": all(
            bool(required_tables.get(table)) for table in REQUIRED_TARGET_TABLES
        ),
        "quick_check": clone.get("quick_check"),
        "operating_database_mutated": False,
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
    if clone.exists() or Path(f"{clone}-wal").exists() or Path(f"{clone}-shm").exists():
        raise FileExistsError(f"clone database artifacts already exist: {clone}")


def _open_read_only(path: Path) -> sqlite3.Connection:
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=ro",
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
    return {
        "schema_version": schema_version,
        "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]),
        "table_count": len(tables),
        "projection_outbox": outbox,
        "required_tables": {table: table in tables for table in REQUIRED_TARGET_TABLES},
    }


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
            fingerprints[label] = {"exists": False, "size": 0, "mtime_ns": None}
            continue
        stat = candidate.stat()
        fingerprints[label] = {
            "exists": True,
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    return fingerprints


def _changed_source_data_files(
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> list[str]:
    changed: list[str] = []
    if before.get("main") != after.get("main"):
        changed.append("<main>")

    wal_before = _mapping(before.get("wal"))
    wal_after = _mapping(after.get("wal"))
    if wal_before.get("exists"):
        if wal_before != wal_after:
            changed.append("-wal")
    elif wal_after.get("exists") and int(wal_after.get("size") or 0) > 0:
        changed.append("-wal")
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
            f"- verdict: `{verdict.get('status')}`",
            f"- source_db: `{source.get('path')}`",
            f"- clone_db: `{clone.get('path')}`",
            "- schema: "
            f"`{source_snapshot.get('schema_version')} -> "
            f"{clone_after.get('schema_version')}`",
            f"- backup_elapsed_sec: `{float(clone.get('backup_elapsed_sec') or 0):.3f}`",
            f"- migration_elapsed_sec: `{float(clone.get('migration_elapsed_sec') or 0):.3f}`",
            f"- idempotent_elapsed_sec: `{float(clone.get('idempotent_elapsed_sec') or 0):.3f}`",
            f"- quick_check_elapsed_sec: `{float(clone.get('quick_check_elapsed_sec') or 0):.3f}`",
            f"- quick_check: `{json.dumps(clone.get('quick_check'))}`",
            "- projection_outbox_before: "
            f"`{json.dumps(source_snapshot.get('projection_outbox'), sort_keys=True)}`",
            "- projection_outbox_after: "
            f"`{json.dumps(clone_after.get('projection_outbox'), sort_keys=True)}`",
            "- source_data_files_unchanged: "
            f"`{str(bool(verdict.get('source_data_files_unchanged'))).lower()}`",
            "- required_tables_present: "
            f"`{str(bool(verdict.get('required_tables_present'))).lower()}`",
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
