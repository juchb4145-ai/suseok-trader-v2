# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domain.broker.utils import new_message_id
from services.runtime.alpha_replay import (
    ALPHA_REPLAY_REPORT_FORMAT,
    REPLAY_MODES,
    run_point_in_time_alpha_replay,
)
from services.runtime.projection_replay import (
    SAFE_REPLAY_EVENT_TYPES,
    export_replay_bundle,
)

DEFAULT_WORK_ROOT = ROOT_DIR / "storage" / "replay" / "alpha"
DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "alpha_replay"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run deterministic point-in-time replay from an immutable/read-only source "
            "into a new isolated database. Trading and order side effects are denied."
        )
    )
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--trade-date")
    parser.add_argument("--mode", choices=REPLAY_MODES, default="ALPHA_REPLAY")
    parser.add_argument("--work-root", default=str(DEFAULT_WORK_ROOT))
    parser.add_argument("--out-dir", default=str(DEFAULT_REPORT_ROOT))
    parser.add_argument("--operational-db")
    parser.add_argument("--commit-sha")
    args = parser.parse_args()

    source_path = Path(args.source_db).expanduser().resolve()
    if not source_path.is_file():
        parser.error(f"source DB does not exist: {source_path}")
    run_id = new_message_id("alpha_replay")
    run_dir = Path(args.work_root).expanduser().resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    before = source_database_fingerprint(source_path)
    snapshot_path = copy_sqlite_snapshot(
        source_path,
        target_path=run_dir / "source-snapshot.sqlite3",
    )
    after_snapshot = source_database_fingerprint(source_path)
    source_unchanged = before == after_snapshot
    bundle = export_replay_bundle(
        source_db_path=snapshot_path,
        bundle_dir=run_dir / "bundle",
        trade_date=args.trade_date,
        event_types=SAFE_REPLAY_EVENT_TYPES,
    )
    after_export = source_database_fingerprint(source_path)
    source_unchanged = source_unchanged and before == after_export
    result = run_point_in_time_alpha_replay(
        bundle_dir=bundle.bundle_dir,
        isolated_db_path=run_dir / "alpha-replay.sqlite3",
        operational_db_path=args.operational_db or source_path,
        mode=args.mode,
        commit_sha=args.commit_sha or current_commit_sha(),
    )
    final = source_database_fingerprint(source_path)
    source_unchanged = source_unchanged and before == final
    report = build_alpha_replay_report(
        result.to_dict(),
        source_db_fingerprint_before=before,
        source_db_fingerprint_after=final,
        source_db_unchanged=source_unchanged,
        source_snapshot_path=snapshot_path,
        run_id=run_id,
    )
    report_paths = write_alpha_replay_report(report, out_dir=Path(args.out_dir))
    print(render_console_summary(report, report_paths=report_paths))
    verdict = report["verdict"]
    return 0 if verdict["status"] in {"PASS", "WARN"} else 2


def source_database_fingerprint(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for label, candidate in (
        ("main", path),
        ("wal", Path(f"{path}-wal")),
        ("shm", Path(f"{path}-shm")),
    ):
        if not candidate.exists():
            result[label] = {"exists": False}
            continue
        stat = candidate.stat()
        result[label] = {
            "exists": True,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    return result


def copy_sqlite_snapshot(source_path: Path, *, target_path: Path) -> Path:
    source = source_path.expanduser().resolve()
    target = target_path.expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"source DB does not exist: {source}")
    if target.exists() or Path(f"{target}-wal").exists() or Path(f"{target}-shm").exists():
        raise FileExistsError(f"SQLite snapshot target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    source_wal = Path(f"{source}-wal")
    if source_wal.is_file():
        shutil.copy2(source_wal, Path(f"{target}-wal"))
    return target


def current_commit_sha() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT_DIR,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN"
    return completed.stdout.strip() or "UNKNOWN"


def build_alpha_replay_report(
    result: Mapping[str, Any],
    *,
    source_db_fingerprint_before: Mapping[str, Any],
    source_db_fingerprint_after: Mapping[str, Any],
    source_db_unchanged: bool,
    source_snapshot_path: Path,
    run_id: str,
) -> dict[str, Any]:
    failures = list(result.get("failures") or [])
    if not source_db_unchanged:
        failures.append("SOURCE_DB_FINGERPRINT_CHANGED")
    warnings = list(result.get("warnings") or [])
    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "format": ALPHA_REPLAY_REPORT_FORMAT,
        "generated_at": _now(),
        "run_id": run_id,
        "verdict": {
            "status": status,
            "failures": sorted(set(failures)),
            "warnings": sorted(set(warnings)),
            "point_in_time_violation_count": int(result.get("point_in_time_violation_count") or 0),
            "source_db_unchanged": source_db_unchanged,
            "no_trading_side_effects": bool(result.get("no_trading_side_effects")),
            "alpha_qualified": bool(result.get("alpha_qualified")),
        },
        "source_database": {
            "read_only": True,
            "opened_by_sqlite": False,
            "snapshot_path": str(source_snapshot_path),
            "snapshot_includes_main_and_wal": True,
            "fingerprint_before": dict(source_db_fingerprint_before),
            "fingerprint_after": dict(source_db_fingerprint_after),
            "unchanged": source_db_unchanged,
            "write_count": 0 if source_db_unchanged else None,
        },
        "replay": dict(result),
        "safety": {
            "observe_only": True,
            "live_sim_allowed": False,
            "live_real_allowed": False,
            "production_db_writes_allowed": False,
            "isolated_database_required": True,
            "sqlite_authorizer_required": True,
            "gateway_command_writes_allowed": False,
            "dry_run_writes_allowed": False,
        },
    }


def write_alpha_replay_report(
    report: Mapping[str, Any],
    *,
    out_dir: Path,
) -> dict[str, Path]:
    run_id = str(report.get("run_id") or "run")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir.expanduser().resolve() / f"{stamp}_{run_id[-8:]}"
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    replay = _mapping(report.get("replay"))
    identity = _mapping(replay.get("identity"))
    lines = [
        "# FAST-2A Point-in-Time Alpha Replay",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- mode: `{replay.get('mode')}`",
        f"- event_count: `{replay.get('event_count')}`",
        f"- result_sha256: `{replay.get('result_sha256')}`",
        f"- source_record_sha256: `{identity.get('source_record_sha256')}`",
        f"- config_sha256: `{identity.get('config_sha256')}`",
        f"- commit_sha: `{identity.get('commit_sha')}`",
        f"- source_db_unchanged: `{verdict.get('source_db_unchanged')}`",
        f"- point_in_time_violation_count: `{verdict.get('point_in_time_violation_count')}`",
        f"- scan_coverage: `{replay.get('scan_coverage')}`",
        f"- missing_sources: `{', '.join(replay.get('missing_sources') or []) or '-'}`",
        f"- alpha_qualified: `{verdict.get('alpha_qualified')}`",
        f"- qualification_reasons: `{', '.join(replay.get('qualification_reasons') or []) or '-'}`",
        f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
        f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
        "",
        "## Safety",
        "",
        "- source DB: strict read-only; before/after main/WAL/SHM fingerprint compared",
        "- replay DB: new isolated file guarded by the projection replay SQLite authorizer",
        "- virtual clock wall-clock reads: `0`",
        "- GatewayCommand/LIVE_SIM/DRY_RUN writes: `0`",
        "- order and broker calls: unavailable",
        "",
        "`FIRST_PAGE_ONLY` or missing market-scan/theme lineage prevents "
        "`ALPHA_QUALIFIED`; it does not hide otherwise valid structural replay evidence.",
    ]
    return "\n".join(lines)


def render_console_summary(
    report: Mapping[str, Any],
    *,
    report_paths: Mapping[str, Path],
) -> str:
    verdict = _mapping(report.get("verdict"))
    replay = _mapping(report.get("replay"))
    return (
        "FAST-2A replay: "
        f"{verdict.get('status')} events={replay.get('event_count')} "
        f"pit_violations={verdict.get('point_in_time_violation_count')} "
        f"source_unchanged={verdict.get('source_db_unchanged')} "
        f"alpha_qualified={verdict.get('alpha_qualified')} "
        f"report={report_paths['summary_md']}"
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
