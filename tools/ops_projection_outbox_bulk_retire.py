from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tools.ops_market_data_tr_response_side_effect_check import (
    fetch_json,
    is_locked_retryable_payload,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply projection_outbox bulk shadow retire."
    )
    parser.add_argument(
        "--core-url",
        default=os.environ.get("TRADING_CORE_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("TRADING_CORE_TOKEN")
        or os.environ.get("GATEWAY_CORE_TOKEN", ""),
    )
    parser.add_argument("--timeout-sec", type=float, default=60.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--older-than-sec", type=int, default=60)
    parser.add_argument("--include-projection-names", default="")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "projection_outbox_bulk_retire"),
    )
    args = parser.parse_args()
    if args.dry_run and args.apply:
        parser.error("use only one of --dry-run or --apply")

    report = run_bulk_retire_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        dry_run=not bool(args.apply),
        limit=args.limit,
        older_than_sec=args.older_than_sec,
        include_projection_names=args.include_projection_names,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_bulk_retire_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    dry_run: bool,
    limit: int,
    older_than_sec: int,
    include_projection_names: str = "",
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    generated_at = (
        datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    before = _fetch_backlog(base_url, token=token, timeout_sec=timeout_sec)
    params = {
        "limit": str(limit),
        "dry_run": "true" if dry_run else "false",
        "older_than_sec": str(older_than_sec),
        "exclude_recent_condition_events": "true",
        "live_safe": "true",
    }
    if include_projection_names.strip():
        params["include_projection_names"] = include_projection_names.strip()
    retire = fetch_json(
        f"{base_url}/api/operator/projection-outbox/bulk-retire?"
        f"{urllib.parse.urlencode(params)}",
        token=token,
        method="POST",
        timeout_sec=timeout_sec,
    )
    after = _fetch_backlog(base_url, token=token, timeout_sec=timeout_sec)
    report = {
        "generated_at": generated_at,
        "core_url": base_url,
        "dry_run": bool(dry_run),
        "limit": int(limit),
        "older_than_sec": int(older_than_sec),
        "before_backlog": before,
        "bulk_retire": retire,
        "after_backlog": after,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in ("before_backlog", "bulk_retire", "after_backlog"):
        payload = report.get(key) or {}
        if is_locked_retryable_payload(payload):
            warnings.append(f"{key.upper()}_LOCKED_RETRYABLE")
            continue
        if not payload.get("ok"):
            failures.append(f"{key.upper()}_API_ERROR")

    before = _data(report, "before_backlog")
    after = _data(report, "after_backlog")
    retire = _data(report, "bulk_retire")
    if int(after.get("error_count") or 0) > 0:
        failures.append("PROJECTION_OUTBOX_ERROR")
    if int(after.get("dead_letter_count") or 0) > 0:
        failures.append("PROJECTION_OUTBOX_DEAD_LETTER")
    if str(after.get("readiness_status") or "").upper() == "FAIL":
        if int(after.get("blocking_pending_count") or 0) > 0:
            failures.append("PROJECTION_OUTBOX_BLOCKING_BACKLOG_REMAINS")
        elif int(after.get("condition_event_blocking_pending_count") or 0) > 0:
            failures.append("CONDITION_EVENT_BLOCKING_BACKLOG_REMAINS")
        else:
            warnings.append("PROJECTION_OUTBOX_BACKLOG_READINESS_FAIL")

    retired_count = int(retire.get("retired_count") or 0)
    if bool(report.get("dry_run")):
        if retired_count <= 0:
            warnings.append("BULK_RETIRE_DRY_RUN_NO_ELIGIBLE_JOBS")
    else:
        before_pending = int(before.get("pending_count") or 0)
        after_pending = int(after.get("pending_count") or 0)
        if retired_count <= 0:
            failures.append("BULK_RETIRE_APPLY_NO_RETIRED_JOBS")
        elif after_pending >= before_pending:
            failures.append("BULK_RETIRE_PENDING_NOT_DECREASED")

    if not bool(report.get("dry_run")) and int(
        after.get("bulk_retire_eligible_count") or 0
    ) > 0:
        warnings.append("BULK_RETIRE_ELIGIBLE_REMAINS")
    if not bool(after.get("pr11_condition_event_cutover_ready")):
        warnings.append("PR11_CONDITION_EVENT_CUTOVER_NOT_READY")

    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(
            failures or not bool(after.get("pr11_condition_event_cutover_ready"))
        ),
        "pr11_condition_event_cutover_ready": bool(
            after.get("pr11_condition_event_cutover_ready")
        ),
    }


def write_report(report: dict[str, Any], *, out_dir: Path) -> dict[str, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir / stamp
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: dict[str, Any]) -> str:
    verdict = report.get("verdict", {})
    retire = _data(report, "bulk_retire")
    after = _data(report, "after_backlog")
    lines = [
        "# Projection Outbox Bulk Retire",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- block_next_pr: `{verdict.get('block_next_pr')}`",
        f"- dry_run: `{report.get('dry_run')}`",
        f"- retired_count: `{retire.get('retired_count')}`",
        f"- applied_count: `{retire.get('applied_count')}`",
        f"- skipped_count: `{retire.get('skipped_count')}`",
        f"- pending_delta: `{retire.get('pending_delta')}`",
        f"- readiness_status: `{after.get('readiness_status')}`",
        f"- blocking_pending_count: `{after.get('blocking_pending_count')}`",
        f"- bulk_retire_eligible_count: `{after.get('bulk_retire_eligible_count')}`",
        f"- condition_event_blocking_pending_count: `{after.get('condition_event_blocking_pending_count')}`",
        f"- pr11_condition_event_cutover_ready: `{after.get('pr11_condition_event_cutover_ready')}`",
        f"- failures: `{verdict.get('failures', [])}`",
        f"- warnings: `{verdict.get('warnings', [])}`",
        "",
        "## Safety",
        "",
        "- This tool updates only projection_outbox status/metadata.",
        "- Projection tables are not mutated.",
        "- effective_skip_inline events are excluded.",
        "- LIVE_REAL/order/safety gates are unchanged.",
    ]
    return "\n".join(lines) + "\n"


def render_console_summary(report: dict[str, Any]) -> str:
    verdict = report.get("verdict", {})
    retire = _data(report, "bulk_retire")
    after = _data(report, "after_backlog")
    return (
        "projection_outbox bulk_retire: "
        f"{verdict.get('status')} "
        f"dry_run={report.get('dry_run')} "
        f"retired={retire.get('retired_count')} "
        f"pending_delta={retire.get('pending_delta')} "
        f"blocking={after.get('blocking_pending_count')} "
        f"eligible={after.get('bulk_retire_eligible_count')} "
        f"pr11_ready={after.get('pr11_condition_event_cutover_ready')}"
    )


def _fetch_backlog(
    base_url: str,
    *,
    token: str,
    timeout_sec: float,
) -> dict[str, Any]:
    return fetch_json(
        f"{base_url}/api/operator/projection-outbox/backlog",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )


def _data(report: dict[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
