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
        description="Check PR-7 market_data price_tick-only append-only cutover."
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
        default=str(ROOT_DIR / "reports" / "market_data_price_tick_cutover"),
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
            f"{urllib.parse.urlencode({'limit': str(limit), 'apply_projection': 'true', 'live_safe': 'true'})}",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )
        run_once_payloads["reconcile"] = fetch_json(
            f"{base_url}/api/operator/market-data-projection-reconcile/run-once?"
            f"{urllib.parse.urlencode({'limit': str(max(limit, 500)), 'live_safe': 'true'})}",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )

    status_payload = fetch_json(
        f"{base_url}/api/operator/market-data-append-only-routing/status",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    decisions_payload = fetch_json(
        f"{base_url}/api/operator/market-data-append-only-routing/decisions?"
        f"{urllib.parse.urlencode({'limit': str(limit)})}",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    outbox_payload = fetch_json(
        f"{base_url}/api/operator/projection-outbox/status",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    reconcile_payload = fetch_json(
        f"{base_url}/api/operator/market-data-projection-reconcile/latest",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    dashboard_params = {
        "sections": "market_data,projection_outbox,pipeline_summary,gateway,errors",
        "detail": "summary",
        "limit": "20",
        "fast": "true",
        "timeout_budget_ms": "5000",
    }
    dashboard_payload = fetch_json(
        f"{base_url}/api/dashboard/snapshot?{urllib.parse.urlencode(dashboard_params)}",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )
    report = {
        "generated_at": generated_at,
        "core_url": base_url,
        "run_once": run_once_payloads,
        "routing_status": status_payload,
        "routing_decisions": decisions_payload,
        "projection_outbox": outbox_payload,
        "latest_reconcile": reconcile_payload,
        "dashboard_snapshot": dashboard_payload,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    run_once_blocker = False
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
    for name, payload in (report.get("run_once") or {}).items():
        if not isinstance(payload, dict):
            continue
        if is_locked_retryable_payload(payload):
            warnings.append(f"{str(name).upper()}_LOCKED_RETRYABLE")
            run_once_blocker = True
        elif not payload.get("ok"):
            failures.append(f"{str(name).upper()}_RUN_ONCE_API_ERROR")

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
    effective_price_tick = int(status.get("effective_price_tick_skip_count") or 0)
    effective_total = int(status.get("effective_skip_inline_count") or 0)
    deferred_count = int(status.get("deferred_incremental_enqueue_count") or 0)
    pending_count = int(outbox.get("pending_count") or 0)
    outbox_error_count = int(outbox.get("error_count") or 0) + int(
        outbox.get("dead_letter_count") or 0
    )

    if int(status.get("condition_event_effective_skip_count") or 0) > 0:
        failures.append("CONDITION_EVENT_EFFECTIVE_SKIP_FORBIDDEN")
    if int(status.get("tr_response_effective_skip_count") or 0) > 0:
        failures.append("TR_RESPONSE_EFFECTIVE_SKIP_FORBIDDEN")
    if int(status.get("invalid_effective_skip_count") or 0) > 0:
        failures.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
    if effective_price_tick > 0 and not bool(status.get("worker_apply_enabled")):
        failures.append("PRICE_TICK_SKIP_WITH_WORKER_APPLY_DISABLED")
    if effective_price_tick > 0 and (
        outbox_error_count > 0
        or int(status.get("effective_skip_outbox_error_count") or 0) > 0
    ):
        failures.append("EFFECTIVE_SKIP_OUTBOX_ERROR_OR_DEAD_LETTER")
    if isinstance(latest_run, dict) and latest_run.get("status") == "FAIL":
        failures.append("LATEST_RECONCILE_FAIL")
    if effective_total > 0 and not bool(status.get("append_only_ready")):
        failures.append("EFFECTIVE_SKIP_WITH_APPEND_ONLY_NOT_READY")
    if (
        effective_price_tick > 0
        and pending_count == 0
        and deferred_count < effective_price_tick
    ):
        failures.append("DEFERRED_INCREMENTAL_ENQUEUE_MISSING")
    if (
        int(dashboard_routing.get("invalid_effective_skip_count") or 0)
        != int(status.get("invalid_effective_skip_count") or 0)
    ):
        failures.append("DASHBOARD_ROUTING_STATUS_MISMATCH")

    if bool(status.get("price_tick_cutover_enabled")) and int(
        status.get("skip_budget_limit_per_minute") or 0
    ) <= 0:
        warnings.append("PRICE_TICK_CUTOVER_ENABLED_BUT_SKIP_BUDGET_ZERO")
    if int(status.get("skip_budget_limit_per_minute") or 0) > 0 and int(
        status.get("skip_budget_remaining_current_minute") or 0
    ) <= 0:
        warnings.append("PRICE_TICK_SKIP_BUDGET_EXHAUSTED")
    if pending_count > 0:
        warnings.append("PROJECTION_OUTBOX_PENDING_WORKER_RUN_ONCE_RECOMMENDED")
    if effective_price_tick > 0 and pending_count > 0 and deferred_count < effective_price_tick:
        warnings.append("DEFERRED_INCREMENTAL_ENQUEUE_WAITING_FOR_WORKER")
    if isinstance(latest_run, dict) and latest_run.get("status") not in {"PASS", "WARN"}:
        warnings.append("LATEST_RECONCILE_NOT_READY")
    if not isinstance(latest_run, dict):
        warnings.append("LATEST_RECONCILE_MISSING")

    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures or run_once_blocker),
    }


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
        "# Market Data Price Tick Cutover Check",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- dry_run_enabled: `{status.get('dry_run_enabled')}`",
        f"- cutover_enabled: `{status.get('cutover_enabled')}`",
        f"- price_tick_cutover_enabled: `{status.get('price_tick_cutover_enabled')}`",
        f"- worker_apply_enabled: `{status.get('worker_apply_enabled')}`",
        f"- append_only_ready: `{status.get('append_only_ready')}`",
        f"- skip_budget_remaining: `{status.get('skip_budget_remaining_current_minute')}`",
        f"- effective_price_tick_skip_count: `{status.get('effective_price_tick_skip_count')}`",
        f"- invalid_effective_skip_count: `{status.get('invalid_effective_skip_count')}`",
        f"- deferred_incremental_enqueue_count: `{status.get('deferred_incremental_enqueue_count')}`",
        f"- failures: `{verdict.get('failures', [])}`",
        f"- warnings: `{verdict.get('warnings', [])}`",
        "",
        "## Safety",
        "",
        "- PR-7 is price_tick-only limited cutover.",
        "- condition_event/tr_response inline projection remains enabled.",
        "- LIVE_SIM/LIVE_REAL/order behavior is unchanged.",
    ]
    return "\n".join(lines) + "\n"


def render_console_summary(report: dict[str, Any]) -> str:
    status = _data(report, "routing_status")
    verdict = report.get("verdict", {})
    return (
        "market_data price_tick cutover: "
        f"{verdict.get('status')} "
        f"price_tick_skip={status.get('effective_price_tick_skip_count')} "
        f"invalid_skip={status.get('invalid_effective_skip_count')} "
        f"deferred_enqueue={status.get('deferred_incremental_enqueue_count')}"
    )


def _data(report: dict[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _json_or_empty(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
