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

from tools.ops_market_data_tr_response_side_effect_check import fetch_json


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check PR-9 tr_response market_data limited cutover."
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
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_data_tr_response_cutover"),
    )
    args = parser.parse_args()

    report = run_cutover_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
        run_once=args.run_once,
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_cutover_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    timeout_sec: float,
    out_dir: Path,
    run_once: bool = False,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    run_once_payloads: dict[str, Any] = {}
    if run_once:
        run_once_payloads["projection_outbox"] = fetch_json(
            f"{base_url}/api/operator/projection-outbox/run-once?"
            f"{urllib.parse.urlencode({'limit': str(limit), 'apply_projection': 'true'})}",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )
        run_once_payloads["reconcile"] = fetch_json(
            f"{base_url}/api/operator/market-data-projection-reconcile/run-once?"
            f"{urllib.parse.urlencode({'limit': str(max(limit, 500))})}",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )

    dashboard_params = {
        "sections": "market_data,projection_outbox,pipeline_summary,gateway,errors",
        "detail": "summary",
        "limit": "20",
        "fast": "true",
        "timeout_budget_ms": "5000",
    }
    report = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "core_url": base_url,
        "run_once": run_once_payloads,
        "routing_status": fetch_json(
            f"{base_url}/api/operator/market-data-append-only-routing/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "routing_decisions": fetch_json(
            f"{base_url}/api/operator/market-data-append-only-routing/decisions?"
            f"{urllib.parse.urlencode({'limit': str(limit)})}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "projection_outbox": fetch_json(
            f"{base_url}/api/operator/projection-outbox/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "latest_reconcile": fetch_json(
            f"{base_url}/api/operator/market-data-projection-reconcile/latest",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "dashboard_snapshot": fetch_json(
            f"{base_url}/api/dashboard/snapshot?{urllib.parse.urlencode(dashboard_params)}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in (
        "routing_status",
        "routing_decisions",
        "projection_outbox",
        "latest_reconcile",
        "dashboard_snapshot",
    ):
        payload = report.get(key) or {}
        if not payload.get("ok"):
            failures.append(f"{key.upper()}_API_ERROR")

    status = _data(report, "routing_status")
    outbox = _data(report, "projection_outbox")
    reconcile = _data(report, "latest_reconcile")
    dashboard = _data(report, "dashboard_snapshot")
    latest_run = reconcile.get("latest_run") if isinstance(reconcile, dict) else None
    dashboard_routing = (
        dashboard.get("pipeline_summary", {}).get("market_data_append_only_routing", {})
        if isinstance(dashboard, dict)
        else {}
    )

    tr_effective = int(status.get("tr_response_effective_skip_count") or 0)
    condition_effective = int(status.get("condition_event_effective_skip_count") or 0)
    invalid_effective = int(status.get("invalid_effective_skip_count") or 0)
    worker_enabled = bool(status.get("worker_apply_enabled"))
    tr_errors = int(
        status.get("tr_response_deferred_quote_refresh_error_count")
        or status.get("tr_response_deferred_side_effect_error_count")
        or 0
    )
    outbox_error_count = int(outbox.get("error_count") or 0) + int(
        outbox.get("dead_letter_count") or 0
    )

    if condition_effective > 0:
        failures.append("CONDITION_EVENT_EFFECTIVE_SKIP_FORBIDDEN")
    if invalid_effective > 0:
        failures.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
    if tr_effective > 0 and not worker_enabled:
        failures.append("TR_RESPONSE_EFFECTIVE_SKIP_WITH_WORKER_APPLY_DISABLED")
    if tr_errors > 0:
        failures.append("TR_RESPONSE_DEFERRED_QUOTE_REFRESH_ERROR")
    if outbox_error_count > 0:
        failures.append("PROJECTION_OUTBOX_ERROR_OR_DEAD_LETTER")
    if isinstance(latest_run, dict) and latest_run.get("status") == "FAIL":
        failures.append("LATEST_RECONCILE_FAIL")
    if (
        int(dashboard_routing.get("tr_response_effective_skip_count") or 0)
        != tr_effective
    ):
        failures.append("DASHBOARD_TR_RESPONSE_ROUTING_STATUS_MISMATCH")
    if tr_effective > 0 and not bool(status.get("append_only_ready")):
        failures.append("TR_RESPONSE_EFFECTIVE_SKIP_WITH_APPEND_ONLY_NOT_READY")

    if tr_effective <= 0:
        warnings.append("NO_TR_RESPONSE_EFFECTIVE_SKIP_OBSERVED")
    if bool(status.get("tr_response_cutover_enabled")) and int(
        status.get("tr_response_skip_budget_remaining_current_minute") or 0
    ) <= 0:
        warnings.append("TR_RESPONSE_SKIP_BUDGET_EXHAUSTED")
    if int(status.get("tr_response_pending_worker_count") or 0) > 0:
        warnings.append("TR_RESPONSE_PENDING_WORKER_WITHIN_SLA")
    if int(outbox.get("pending_count") or 0) > 0:
        warnings.append("PROJECTION_OUTBOX_PENDING_WORKER_RUN_ONCE_RECOMMENDED")
    if not isinstance(latest_run, dict):
        warnings.append("LATEST_RECONCILE_MISSING")
    elif latest_run.get("status") not in {"PASS", "WARN"}:
        warnings.append("LATEST_RECONCILE_NOT_READY")
    if int(status.get("tr_response_deferred_quote_refresh_count") or 0) <= 0:
        warnings.append("TR_RESPONSE_DEFERRED_QUOTE_REFRESH_NOT_OBSERVED")

    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
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
    status = _data(report, "routing_status")
    verdict = report.get("verdict", {})
    lines = [
        "# Market Data TR Response Cutover Check",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        _summary_line(status, "tr_response_cutover_enabled"),
        _summary_line(status, "tr_response_skip_budget_remaining_current_minute"),
        _summary_line(status, "tr_response_effective_skip_count"),
        _summary_line(status, "tr_response_pending_worker_count"),
        _summary_line(status, "tr_response_worker_applied_count"),
        _summary_line(status, "tr_response_deferred_quote_refresh_count"),
        _summary_line(status, "tr_response_deferred_quote_refresh_error_count"),
        _summary_line(status, "condition_event_effective_skip_count"),
        _summary_line(status, "invalid_effective_skip_count"),
        f"- failures: `{verdict.get('failures', [])}`",
        f"- warnings: `{verdict.get('warnings', [])}`",
        "",
        "## Safety",
        "",
        "- PR-9 allows tr_response inline skip only behind strict flags and budget.",
        "- condition_event inline projection remains enabled.",
        "- LIVE_SIM/LIVE_REAL/order behavior is unchanged.",
    ]
    return "\n".join(lines) + "\n"


def render_console_summary(report: dict[str, Any]) -> str:
    status = _data(report, "routing_status")
    verdict = report.get("verdict", {})
    return (
        "market_data tr_response cutover: "
        f"{verdict.get('status')} "
        f"tr_effective_skip={status.get('tr_response_effective_skip_count')} "
        f"worker_applied={status.get('tr_response_worker_applied_count')} "
        f"deferred={status.get('tr_response_deferred_quote_refresh_count')} "
        f"errors={status.get('tr_response_deferred_quote_refresh_error_count')}"
    )


def _summary_line(status: dict[str, Any], key: str) -> str:
    return f"- {key}: `{status.get(key)}`"


def _data(report: dict[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
