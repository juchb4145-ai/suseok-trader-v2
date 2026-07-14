from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Archive one failed append-only evidence session without rewriting it."
    )
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--session-state-path", default="")
    parser.add_argument("--latest-report-path", required=True)
    parser.add_argument("--summary-report-path", required=True)
    parser.add_argument("--archive-root", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    session_state_path = (
        Path(args.session_state_path).resolve()
        if args.session_state_path
        else Path(f"{db_path}.session.json")
    )
    archive_root = (
        Path(args.archive_root).resolve()
        if args.archive_root
        else db_path.parent / "archive"
    )
    result = archive_failed_evidence_session(
        db_path=db_path,
        session_state_path=session_state_path,
        latest_report_path=Path(args.latest_report_path).resolve(),
        summary_report_path=Path(args.summary_report_path).resolve(),
        archive_root=archive_root,
        apply=bool(args.apply),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def archive_failed_evidence_session(
    *,
    db_path: Path,
    session_state_path: Path,
    latest_report_path: Path,
    summary_report_path: Path,
    archive_root: Path,
    apply: bool,
) -> dict[str, Any]:
    db_path = db_path.resolve()
    session_state_path = session_state_path.resolve()
    latest_report_path = latest_report_path.resolve()
    summary_report_path = summary_report_path.resolve()
    archive_root = archive_root.resolve()
    session = _load_json_object(session_state_path, label="session state")
    report = _load_json_object(latest_report_path, label="latest report")
    _validate_failed_session(
        db_path=db_path,
        session=session,
        report=report,
        summary_report_path=summary_report_path,
    )

    trade_date = str(session["trade_date"])
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    archive_dir = archive_root / trade_date / f"failed_session_{stamp}"
    archived_db_path = archive_dir / f"{db_path.stem}.failed-{trade_date}{db_path.suffix}"
    archived_session_path = archive_dir / "session-state.json"
    archived_latest_report_path = archive_dir / "latest-report.json"
    archived_summary_report_path = archive_dir / "summary-report.md"
    manifest_path = archive_dir / "manifest.json"
    planned = {
        "status": "DRY_RUN" if not apply else "PREPARED",
        "applied": bool(apply),
        "trade_date": trade_date,
        "source_db_path": str(db_path),
        "source_session_state_path": str(session_state_path),
        "source_latest_report_path": str(latest_report_path),
        "source_summary_report_path": str(summary_report_path),
        "archive_dir": str(archive_dir),
        "archived_db_path": str(archived_db_path),
        "archived_session_state_path": str(archived_session_path),
        "manifest_path": str(manifest_path),
        "report_verdict": str(_mapping(report.get("verdict")).get("status") or ""),
        "report_failures": list(_mapping(report.get("verdict")).get("failures") or []),
        "source_preserved_without_row_rewrite": True,
    }
    if not apply:
        return planned
    if archive_dir.exists():
        raise ValueError(f"archive directory already exists: {archive_dir}")
    _require_same_volume(db_path, archive_root)

    checkpoint = _checkpoint_and_quick_check(db_path)
    source_size = db_path.stat().st_size
    source_hash = _sha256(db_path)
    archive_dir.mkdir(parents=True, exist_ok=False)
    shutil.copy2(latest_report_path, archived_latest_report_path)
    shutil.copy2(summary_report_path, archived_summary_report_path)

    moved_sidecars: list[tuple[Path, Path]] = []
    try:
        os.replace(db_path, archived_db_path)
        moved_sidecars.append((archived_db_path, db_path))
        for suffix in ("-wal", "-shm"):
            source = Path(f"{db_path}{suffix}")
            if source.exists():
                destination = Path(f"{archived_db_path}{suffix}")
                os.replace(source, destination)
                moved_sidecars.append((destination, source))
        archived_quick_check = _quick_check(archived_db_path)
        os.replace(session_state_path, archived_session_path)
        moved_sidecars.append((archived_session_path, session_state_path))
    except Exception:
        for archived, source in reversed(moved_sidecars):
            if archived.exists() and not source.exists():
                os.replace(archived, source)
        raise

    manifest = {
        **planned,
        "status": "COMPLETED",
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source_size_bytes": source_size,
        "source_sha256": source_hash,
        "archived_sha256": _sha256(archived_db_path),
        "checkpoint": checkpoint,
        "source_quick_check": checkpoint["quick_check"],
        "archived_quick_check": archived_quick_check,
        "canonical_db_released": not db_path.exists(),
        "canonical_session_released": not session_state_path.exists(),
    }
    _write_json_atomic(manifest_path, manifest)
    return manifest


def _validate_failed_session(
    *,
    db_path: Path,
    session: Mapping[str, Any],
    report: Mapping[str, Any],
    summary_report_path: Path,
) -> None:
    if not db_path.is_file():
        raise ValueError(f"evidence DB does not exist: {db_path}")
    if not summary_report_path.is_file():
        raise ValueError(f"summary report does not exist: {summary_report_path}")
    trade_date = str(session.get("trade_date") or "")
    if not trade_date:
        raise ValueError("session trade_date is required")
    if _normalize_path(session.get("database_path")) != _normalize_path(db_path):
        raise ValueError("session database path does not match evidence DB")
    if str(report.get("trade_date") or "") != trade_date:
        raise ValueError("report trade_date does not match session")
    if _normalize_path(report.get("expected_db_path")) != _normalize_path(db_path):
        raise ValueError("report database path does not match evidence DB")
    verdict = _mapping(report.get("verdict"))
    if str(verdict.get("status") or "").upper() != "FAIL":
        raise ValueError("only a failed evidence report can be archived")


def _checkpoint_and_quick_check(db_path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(str(db_path), timeout=5.0)
    try:
        connection.execute("PRAGMA busy_timeout = 5000")
        checkpoint_row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        checkpoint = tuple(int(value) for value in (checkpoint_row or ()))
        if len(checkpoint) != 3 or checkpoint[0] != 0:
            raise ValueError(f"WAL checkpoint did not complete: {checkpoint}")
        quick_check = _quick_check_connection(connection)
        return {
            "busy": checkpoint[0],
            "log_frames": checkpoint[1],
            "checkpointed_frames": checkpoint[2],
            "quick_check": quick_check,
        }
    finally:
        connection.close()


def _quick_check(db_path: Path) -> str:
    connection = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        return _quick_check_connection(connection)
    finally:
        connection.close()


def _quick_check_connection(connection: sqlite3.Connection) -> str:
    rows = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
    if rows != ["ok"]:
        raise ValueError(f"SQLite quick_check failed: {rows[:10]}")
    return "ok"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"{label} does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"{label} must contain a JSON object")
    return dict(payload)


def _require_same_volume(source: Path, archive_root: Path) -> None:
    source_drive = source.drive.casefold()
    archive_drive = archive_root.drive.casefold()
    if source_drive and archive_drive and source_drive != archive_drive:
        raise ValueError("archive root must be on the same volume for atomic archival")


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _normalize_path(value: object) -> str:
    if value in (None, ""):
        return ""
    return os.path.normcase(os.path.abspath(os.fspath(value)))


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
