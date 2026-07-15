from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.pipeline_coherency import build_pipeline_coherency_rca_status  # noqa: E402
from services.pipeline_coherency_disposition import (  # noqa: E402
    resolve_pipeline_coherency_dispositions,
)

_PAGE_LIMIT = 500
_DATA_FILE_SUFFIXES = ("", "-wal", "-shm", "-journal")
_SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"(?i)(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}"),
    re.compile(r"(?i)github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}=*"),
    re.compile(r"(?i)(?:token|password|secret|api[_-]?key|account)\s*[:=]\s*\S+"),
    re.compile(r"(?i)(?:acct|account|계좌)[_.:@/-]?\d{6,16}"),
    re.compile(r"(?<![A-Za-z0-9])\d{8,16}(?![A-Za-z0-9])"),
)
_HYPHENATED_ACCOUNT_PATTERN = re.compile(
    r"(?<!\d)\d{3,6}-\d{2,6}(?:-\d{1,6})?(?!\d)"
)
_ISO_DATE_PATTERN = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)")


class PipelineCoherencyRcaError(RuntimeError):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build full-count FAST-0 pipeline RCA evidence from a strict read-only "
            "SQLite snapshot without starting Core or Gateway."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--trade-date")
    parser.add_argument("--candidate-instance-id")
    parser.add_argument("--max-age-sec", type=float, default=60.0)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "pipeline_coherency_rca"),
    )
    args = parser.parse_args()
    try:
        report = run_report(
            db_path=Path(args.db),
            trade_date=args.trade_date,
            candidate_instance_id=args.candidate_instance_id,
            max_age_sec=max(float(args.max_age_sec), 0.0),
            run_quick_check=True,
            out_dir=Path(args.out_dir),
        )
    except (OSError, sqlite3.Error, PipelineCoherencyRcaError) as exc:
        print(f"pipeline coherency RCA: ERROR {exc}", file=sys.stderr)
        return 2
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] == "PASS" else 2


def run_report(
    *,
    db_path: Path,
    trade_date: str | None,
    candidate_instance_id: str | None,
    max_age_sec: float,
    run_quick_check: bool,
    out_dir: Path,
) -> dict[str, Any]:
    if run_quick_check is not True:
        raise PipelineCoherencyRcaError("DATABASE_QUICK_CHECK_IS_MANDATORY")
    resolved_path = _validated_database_path(db_path)
    files_before = _file_fingerprints(resolved_path)
    observed_at = datetime.now(UTC)
    connection = _open_strict_read_only(resolved_path)
    try:
        quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
        connection.execute("BEGIN DEFERRED")
        try:
            rca = _read_all_pages(
                connection,
                trade_date=trade_date,
                candidate_instance_id=candidate_instance_id,
                max_age_sec=max_age_sec,
                as_of=observed_at,
            )
        finally:
            connection.rollback()
    finally:
        connection.close()
    _validated_database_path(resolved_path)
    _assert_no_sidecars(resolved_path)
    files_after = _file_fingerprints(resolved_path)
    report: dict[str, Any] = {
        "contract": "fast0-pipeline-coherency-rca-evidence.v1",
        "generated_at": _wire(observed_at),
        "database": {
            "path": str(resolved_path),
            "files_before": files_before,
            "files_after": files_after,
            "quick_check": quick_check,
        },
        "pipeline_rca": rca,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "core_started": False,
        "gateway_started": False,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    return report


def _read_all_pages(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    candidate_instance_id: str | None,
    max_age_sec: float,
    as_of: datetime,
) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    offset = 0
    first: dict[str, Any] | None = None
    while True:
        page = build_pipeline_coherency_rca_status(
            connection,
            trade_date=trade_date,
            max_age_sec=max_age_sec,
            limit=_PAGE_LIMIT,
            offset=offset,
            candidate_instance_id=candidate_instance_id,
            disposition_resolver=(
                lambda resolved_trade_date, subjects: resolve_pipeline_coherency_dispositions(
                    connection,
                    resolved_trade_date,
                    subjects,
                    as_of=as_of,
                )
            ),
            as_of=as_of,
        )
        if first is None:
            first = page
        pages.append(
            {
                "offset": page.get("offset"),
                "returned_count": page.get("returned_count"),
                "full_count": page.get("full_count"),
                "inventory_digest": page.get("inventory_digest"),
                "inventory_end_digest": page.get("inventory_end_digest"),
                "inventory_count_consistent": page.get("inventory_count_consistent"),
            }
        )
        page_items = page.get("items")
        if not isinstance(page_items, Sequence) or isinstance(page_items, (str, bytes)):
            raise PipelineCoherencyRcaError("PIPELINE_RCA_ITEMS_INVALID")
        items.extend(dict(item) for item in page_items if isinstance(item, Mapping))
        if not bool(page.get("has_more")):
            break
        next_offset = page.get("next_offset")
        if not isinstance(next_offset, int) or next_offset <= offset:
            raise PipelineCoherencyRcaError("PIPELINE_RCA_PAGINATION_STALLED")
        offset = next_offset
    assert first is not None
    result = {key: value for key, value in first.items() if key != "items"}
    result["items"] = items
    result["pages"] = pages
    result["page_count"] = len(pages)
    result["collected_count"] = len(items)
    return result


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    database = _mapping(report.get("database"))
    rca = _mapping(report.get("pipeline_rca"))
    failures: list[str] = []
    before = database.get("files_before")
    after = database.get("files_after")
    if not isinstance(before, Mapping) or before != after:
        failures.append("DATABASE_DATA_FILE_CHANGED")
    quick_check = database.get("quick_check")
    if quick_check != ["ok"]:
        failures.append("DATABASE_QUICK_CHECK_FAILED")
    if rca.get("inventory_count_consistent") is not True:
        failures.append("PIPELINE_INVENTORY_INCONSISTENT")
    if rca.get("inventory_digest") != rca.get("inventory_end_digest"):
        failures.append("PIPELINE_INVENTORY_CHANGED_DURING_SNAPSHOT")
    full_count = _integer(rca.get("full_count"), default=-1)
    collected_count = _integer(rca.get("collected_count"), default=-2)
    items = rca.get("items")
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes)):
        failures.append("PIPELINE_ITEMS_INVALID")
        items = []
    candidate_ids = [
        str(item.get("candidate_instance_id") or "") for item in items if isinstance(item, Mapping)
    ]
    if full_count != collected_count or collected_count != len(candidate_ids):
        failures.append("PIPELINE_FULL_COUNT_MISMATCH")
    if len(candidate_ids) != len(set(candidate_ids)):
        failures.append("PIPELINE_DUPLICATE_SUBJECT")
    pages = rca.get("pages")
    if not isinstance(pages, Sequence) or not pages:
        failures.append("PIPELINE_PAGINATION_EVIDENCE_MISSING")
    else:
        for page in pages:
            if not isinstance(page, Mapping):
                failures.append("PIPELINE_PAGINATION_EVIDENCE_INVALID")
                continue
            if (
                page.get("full_count") != full_count
                or page.get("inventory_digest") != rca.get("inventory_digest")
                or page.get("inventory_end_digest") != rca.get("inventory_digest")
                or page.get("inventory_count_consistent") is not True
            ):
                failures.append("PIPELINE_PAGE_SNAPSHOT_MISMATCH")
    qualification_status = str(rca.get("qualification_status") or "UNKNOWN").upper()
    if qualification_status != "PASS":
        failures.append("PIPELINE_QUALIFICATION_NOT_PASS")
    if rca.get("schema_ready") is not True:
        failures.append("PIPELINE_DISPOSITION_SCHEMA_NOT_READY")
    if report.get("read_only") is not True or report.get("no_order_side_effects") is not True:
        failures.append("READ_ONLY_CONTRACT_INVALID")
    return {
        "status": "PASS" if not failures else "BLOCKED",
        "failures": sorted(set(failures)),
        "qualification_status": qualification_status,
        "qualification_reason_codes": list(rca.get("qualification_reason_codes") or []),
        "canonical_status": rca.get("canonical_status"),
        "full_count": full_count,
        "collected_count": collected_count,
        "inventory_digest": rca.get("inventory_digest"),
        "database_files_unchanged": before == after,
        "read_only": True,
        "no_order_side_effects": True,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(_redact(report), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    rca = _mapping(report.get("pipeline_rca"))
    return "\n".join(
        [
            "# FAST-0 Pipeline Coherency RCA",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- canonical_status: `{verdict.get('canonical_status')}`",
            f"- qualification_status: `{verdict.get('qualification_status')}`",
            f"- full/collected: `{verdict.get('full_count')}/{verdict.get('collected_count')}`",
            f"- page_count: `{rca.get('page_count')}`",
            f"- inventory_digest: `{verdict.get('inventory_digest')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            "",
            "The database was opened immutable/query-only; Core and Gateway were not started.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "pipeline coherency RCA: "
        f"{verdict.get('status')} qualification={verdict.get('qualification_status')} "
        f"full={verdict.get('full_count')} collected={verdict.get('collected_count')}"
    )


def _validated_database_path(path: Path) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
        stat_result = resolved.stat()
    except OSError as exc:
        raise PipelineCoherencyRcaError("DATABASE_NOT_FOUND") from exc
    if not resolved.is_file():
        raise PipelineCoherencyRcaError("DATABASE_NOT_FOUND")
    if int(stat_result.st_nlink) != 1:
        raise PipelineCoherencyRcaError("DATABASE_HARDLINK_ALIAS_UNSAFE")
    return resolved


def _open_strict_read_only(path: Path) -> sqlite3.Connection:
    _assert_no_sidecars(path)
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=ro&immutable=1",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _assert_no_sidecars(path: Path) -> None:
    if any(Path(f"{path}{suffix}").exists() for suffix in ("-wal", "-shm", "-journal")):
        raise PipelineCoherencyRcaError(
            "STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE"
        )


def _file_fingerprints(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for suffix in _DATA_FILE_SUFFIXES:
        candidate = Path(f"{path}{suffix}")
        key = "main" if not suffix else suffix.removeprefix("-")
        if not candidate.exists():
            result[key] = {"exists": False, "size": 0, "sha256": None}
            continue
        stat_before = candidate.stat()
        digest = hashlib.sha256()
        with candidate.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        stat_after = candidate.stat()
        if (
            stat_before.st_size != stat_after.st_size
            or stat_before.st_mtime_ns != stat_after.st_mtime_ns
        ):
            raise PipelineCoherencyRcaError("DATABASE_FILE_CHANGED_DURING_HASH")
        result[key] = {
            "exists": True,
            "size": int(stat_after.st_size),
            "mtime_ns": int(stat_after.st_mtime_ns),
            "sha256": digest.hexdigest(),
        }
    return result


def _redact(value: Any, *, key: str = "") -> Any:
    lowered = key.lower()
    if any(part in lowered for part in ("account", "token", "password", "secret")):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(child): _redact(item, key=str(child)) for child, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, str) and lowered.endswith("_json"):
        try:
            loaded = json.loads(value)
        except (TypeError, ValueError):
            return "[REDACTED]"
        return json.dumps(
            _redact(loaded),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    if isinstance(value, str) and _contains_sensitive_value(value):
        return "[REDACTED]"
    return value


def _contains_sensitive_value(value: str) -> bool:
    if any(pattern.search(value) is not None for pattern in _SENSITIVE_VALUE_PATTERNS):
        return True
    without_iso_dates = _ISO_DATE_PATTERN.sub("", value)
    return _HYPHENATED_ACCOUNT_PATTERN.search(without_iso_dates) is not None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _integer(value: Any, *, default: int) -> int:
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else default


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
