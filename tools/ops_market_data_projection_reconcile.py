from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tools.ops_market_data_tr_response_side_effect_check import (
    fetch_json as _fetch_json_with_locked_retry,
    is_locked_retryable_payload,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run market_data projection reconciliation through Core API."
    )
    parser.add_argument(
        "--core-url",
        default=os.environ.get("TRADING_CORE_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("TRADING_CORE_TOKEN") or os.environ.get("GATEWAY_CORE_TOKEN", ""),
    )
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--min-event-rowid", type=int, default=None)
    parser.add_argument("--max-event-rowid", type=int, default=None)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_data_projection_reconcile"),
    )
    args = parser.parse_args()

    report = run_reconcile_report(
        core_url=args.core_url,
        token=args.token,
        run_once=args.run_once,
        limit=args.limit,
        min_event_rowid=args.min_event_rowid,
        max_event_rowid=args.max_event_rowid,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    status = _latest_status(report)
    return 0 if status in {"PASS", "WARN", None} else 2


def run_reconcile_report(
    *,
    core_url: str,
    token: str,
    run_once: bool,
    limit: int,
    min_event_rowid: int | None = None,
    max_event_rowid: int | None = None,
    timeout_sec: float = 30.0,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    run_once_payload: dict[str, Any] | None = None
    if run_once:
        query = {"limit": str(limit), "persist": "true", "live_safe": "true"}
        if min_event_rowid is not None:
            query["min_event_rowid"] = str(min_event_rowid)
        if max_event_rowid is not None:
            query["max_event_rowid"] = str(max_event_rowid)
        run_once_payload = fetch_json(
            f"{base_url}/api/operator/market-data-projection-reconcile/run-once?"
            f"{urllib.parse.urlencode(query)}",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )
    latest_payload = fetch_json(
        f"{base_url}/api/operator/market-data-projection-reconcile/latest",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report = {
        "generated_at": generated_at,
        "core_url": base_url,
        "run_once": run_once_payload,
        "latest": latest_payload,
        "run_once_locked_retryable": (
            False
            if not isinstance(run_once_payload, dict)
            else is_locked_retryable_payload(run_once_payload)
        ),
    }
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def fetch_json(
    url: str,
    *,
    token: str,
    method: str,
    timeout_sec: float,
) -> dict[str, Any]:
    return _fetch_json_with_locked_retry(
        url,
        token=token,
        method=method,
        timeout_sec=timeout_sec,
    )


def write_report(report: dict[str, Any], *, out_dir: Path) -> dict[str, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir / stamp
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    latest_path = report_dir / "latest.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    latest_path.write_text(
        json.dumps(report.get("latest", {}), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "latest_json": latest_path, "summary_md": summary_path}


def render_markdown_summary(report: dict[str, Any]) -> str:
    latest = _latest_run(report)
    issues = _latest_issues(report)
    lines = [
        "# Market Data Projection Reconcile",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- status: `{None if latest is None else latest.get('status')}`",
        f"- append_only_ready: `{False if latest is None else latest.get('append_only_ready')}`",
        f"- checked_event_count: `{0 if latest is None else latest.get('checked_event_count')}`",
        f"- event_rowid_range: `{None if latest is None else latest.get('event_rowid_min')}`"
        f" - `{None if latest is None else latest.get('event_rowid_max')}`",
        f"- outbox_pending_count: `{0 if latest is None else latest.get('outbox_pending_count')}`",
        f"- outbox_error_count: `{0 if latest is None else latest.get('outbox_error_count')}`",
        f"- dead_letter_count: `{0 if latest is None else latest.get('outbox_dead_letter_count')}`",
        "- missing_projection_count: "
        f"`{0 if latest is None else latest.get('missing_projection_count')}`",
        f"- watermark_risk_count: `{0 if latest is None else latest.get('watermark_risk_count')}`",
        "- synthetic_child_event_issue_count: "
        f"`{0 if latest is None else latest.get('synthetic_child_event_issue_count')}`",
        f"- reason_codes: `{[] if latest is None else latest.get('reason_codes')}`",
        "",
        "## Top Issues",
        "",
    ]
    if not issues:
        lines.append("- none")
    else:
        for issue in issues[:10]:
            lines.append(
                "- "
                f"`{issue.get('severity')}` "
                f"`{issue.get('reason_code')}` "
                f"`{issue.get('event_id')}` "
                f"{issue.get('message')}"
            )
    lines.extend(
        [
            "",
            "## Next Action",
            "",
            "- append-only gateway mode remains disabled.",
            "- Gateway inline projection remains enabled.",
            "- Investigate every FAIL reason before any append-only dry-run flag.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_console_summary(report: dict[str, Any]) -> str:
    latest = _latest_run(report)
    if latest is None:
        return "market_data projection reconcile: no latest run"
    return (
        "market_data projection reconcile: "
        f"{latest.get('status')} "
        f"append_only_ready={latest.get('append_only_ready')} "
        f"checked={latest.get('checked_event_count')} "
        f"missing={latest.get('missing_projection_count')} "
        f"watermark_risk={latest.get('watermark_risk_count')}"
    )


def _latest_status(report: dict[str, Any]) -> str | None:
    latest = _latest_run(report)
    return None if latest is None else str(latest.get("status"))


def _latest_run(report: dict[str, Any]) -> dict[str, Any] | None:
    latest = report.get("latest")
    if not isinstance(latest, dict):
        return None
    data = latest.get("data")
    if not isinstance(data, dict):
        return None
    run = data.get("latest_run")
    return run if isinstance(run, dict) else None


def _latest_issues(report: dict[str, Any]) -> list[dict[str, Any]]:
    latest = report.get("latest")
    if not isinstance(latest, dict):
        return []
    data = latest.get("data")
    if not isinstance(data, dict):
        return []
    issues = data.get("issues")
    return list(issues) if isinstance(issues, list) else []


def _json_or_empty(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
