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
        description="Check PR-10 condition_event worker-side condition_fusion prep."
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
        default=str(ROOT_DIR / "reports" / "market_data_condition_event_side_effect"),
    )
    args = parser.parse_args()

    report = run_side_effect_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
        run_once=args.run_once,
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_side_effect_report(
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
    run_once_blocker = False
    for key in (
        "routing_status",
        "projection_outbox",
        "latest_reconcile",
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
    dashboard_payload = report.get("dashboard_snapshot") or {}
    dashboard_ok = bool(dashboard_payload.get("ok"))
    dashboard_blocker = False
    if not dashboard_ok:
        warnings.append("DASHBOARD_SNAPSHOT_API_ERROR")
        dashboard_blocker = True

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

    condition_effective = int(status.get("condition_event_effective_skip_count") or 0)
    invalid_effective = int(status.get("invalid_effective_skip_count") or 0)
    candidate_ingest_count = int(
        status.get("condition_event_candidate_ingest_executed_count") or 0
    )
    deferred_error_count = int(
        status.get("condition_event_deferred_fusion_refresh_error_count")
        or status.get("condition_event_deferred_side_effect_error_count")
        or 0
    )
    duplicate_count = int(status.get("condition_event_side_effect_duplicate_count") or 0)
    deferred_count = int(
        status.get("condition_event_deferred_fusion_refresh_count")
        or status.get("condition_event_deferred_side_effect_count")
        or 0
    )
    would_skip_count = int(status.get("condition_event_would_skip_inline_count") or 0)
    outbox_error_count = int(outbox.get("error_count") or 0) + int(
        outbox.get("dead_letter_count") or 0
    )

    if condition_effective > 0:
        failures.append("CONDITION_EVENT_EFFECTIVE_SKIP_FORBIDDEN")
    if invalid_effective > 0:
        failures.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
    if candidate_ingest_count > 0:
        failures.append("CONDITION_EVENT_CANDIDATE_INGEST_IN_WORKER")
    if deferred_error_count > 0:
        failures.append("CONDITION_EVENT_DEFERRED_FUSION_REFRESH_ERROR")
    if duplicate_count > 0:
        failures.append("CONDITION_EVENT_SIDE_EFFECT_DUPLICATED")
    if outbox_error_count > 0:
        failures.append("PROJECTION_OUTBOX_ERROR_OR_DEAD_LETTER")
    if isinstance(latest_run, dict) and latest_run.get("status") == "FAIL":
        failures.append("LATEST_RECONCILE_FAIL")
    if dashboard_ok and (
        int(dashboard_routing.get("condition_event_effective_skip_count") or 0)
        != condition_effective
    ):
        failures.append("DASHBOARD_CONDITION_EVENT_ROUTING_STATUS_MISMATCH")
    if dashboard_ok:
        dashboard_warnings = [
            str(warning) for warning in dashboard.get("warnings", []) if warning
        ]
        skipped_timeout = any(
            "SKIPPED_TIMEOUT_BUDGET" in warning for warning in dashboard_warnings
        ) or any(
            str(item.get("reason")) == "SKIPPED_TIMEOUT_BUDGET"
            for item in dashboard.get("skipped_sections", [])
            if isinstance(item, dict)
        )
        if skipped_timeout:
            warnings.append("DASHBOARD_SECTION_SKIPPED_TIMEOUT_BUDGET")
            dashboard_blocker = True
        dashboard_latency_ms = float(dashboard.get("total_latency_ms") or 0)
        if dashboard_latency_ms > 3000:
            warnings.append("DASHBOARD_SNAPSHOT_LATENCY_WARN")

    if not bool(status.get("condition_event_worker_side_effect_ready")):
        warnings.append("CONDITION_EVENT_WORKER_SIDE_EFFECT_NOT_READY")
    if not bool(status.get("condition_event_fusion_enabled")):
        warnings.append("CONDITION_FUSION_INCREMENTAL_DISABLED")
    if would_skip_count == 0 and deferred_count == 0:
        warnings.append("NO_CONDITION_EVENT_EVENTS_OBSERVED")
    if deferred_count == 0:
        warnings.append("CONDITION_EVENT_DEFERRED_FUSION_REFRESH_NOT_OBSERVED")
    if int(outbox.get("pending_count") or 0) > 0:
        warnings.append("PROJECTION_OUTBOX_PENDING_WORKER_RUN_ONCE_RECOMMENDED")
    if not isinstance(latest_run, dict):
        warnings.append("LATEST_RECONCILE_MISSING")
    elif latest_run.get("status") not in {"PASS", "WARN"}:
        warnings.append("LATEST_RECONCILE_NOT_READY")

    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures or dashboard_blocker or run_once_blocker),
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
        "# Market Data Condition Event Side Effect Check",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- block_next_pr: `{verdict.get('block_next_pr')}`",
        _summary_line(status, "condition_event_worker_side_effect_ready"),
        _summary_line(status, "condition_event_fusion_enabled"),
        _summary_line(status, "condition_event_would_skip_inline_count"),
        _summary_line(status, "condition_event_effective_skip_count"),
        _summary_line(status, "condition_event_deferred_fusion_refresh_count"),
        _summary_line(status, "condition_event_deferred_fusion_refresh_error_count"),
        _summary_line(status, "condition_event_candidate_ingest_executed_count"),
        _summary_line(status, "condition_event_side_effect_duplicate_count"),
        f"- failures: `{verdict.get('failures', [])}`",
        f"- warnings: `{verdict.get('warnings', [])}`",
        "",
        "## Safety",
        "",
        "- PR-10 is not a condition_event cutover.",
        "- condition_event inline projection remains enabled.",
        "- candidate ingest remains outside projection_outbox worker.",
        "- LIVE_SIM/LIVE_REAL/order behavior is unchanged.",
    ]
    return "\n".join(lines) + "\n"


def render_console_summary(report: dict[str, Any]) -> str:
    status = _data(report, "routing_status")
    verdict = report.get("verdict", {})
    return (
        "market_data condition_event side-effect: "
        f"{verdict.get('status')} "
        f"condition_skip={status.get('condition_event_effective_skip_count')} "
        f"deferred={status.get('condition_event_deferred_fusion_refresh_count')} "
        f"deferred_errors={status.get('condition_event_deferred_fusion_refresh_error_count')} "
        f"candidate_ingest={status.get('condition_event_candidate_ingest_executed_count')}"
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
