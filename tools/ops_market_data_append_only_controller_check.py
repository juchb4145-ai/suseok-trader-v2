from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tools.ops_market_data_tr_response_side_effect_check import (  # noqa: E402
    fetch_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check PR-12 market_data append-only controller health."
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
    parser.add_argument("--persist-snapshot", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_data_append_only_controller"),
    )
    args = parser.parse_args()

    report = run_controller_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
        persist_snapshot=args.persist_snapshot,
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_controller_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    timeout_sec: float,
    out_dir: Path,
    persist_snapshot: bool = False,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_params = {
        "sections": (
            "gateway,market_data,projection_outbox,projection_outbox_backlog,"
            "market_data_append_only_controller,pipeline_summary,errors"
        ),
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
        "controller_status": fetch_json(
            f"{base_url}/api/operator/market-data-append-only/controller/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
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
        "projection_outbox_backlog": fetch_json(
            f"{base_url}/api/operator/projection-outbox/backlog",
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
        "snapshot": {},
    }
    if persist_snapshot:
        report["snapshot"] = fetch_json(
            f"{base_url}/api/operator/market-data-append-only/controller/snapshot",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in (
        "controller_status",
        "routing_status",
        "projection_outbox",
        "projection_outbox_backlog",
        "latest_reconcile",
        "dashboard_snapshot",
    ):
        payload = report.get(key) or {}
        if not payload.get("ok"):
            failures.append(f"{key.upper()}_API_ERROR")
    snapshot = report.get("snapshot") or {}
    if snapshot and not snapshot.get("ok"):
        failures.append("CONTROLLER_SNAPSHOT_API_ERROR")

    controller = _data(report, "controller_status")
    routing = _data(report, "routing_status")
    outbox = _data(report, "projection_outbox")
    backlog = _data(report, "projection_outbox_backlog")
    reconcile = _data(report, "latest_reconcile")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_controller = (
        dashboard.get("pipeline_summary", {}).get("market_data_append_only_controller", {})
        if isinstance(dashboard, dict)
        else {}
    )
    latest_run = reconcile.get("latest_run") if isinstance(reconcile, dict) else None
    operating_mode = str(controller.get("operating_mode") or "OFF")
    backlog_status = str(
        controller.get("backlog_readiness_status")
        or backlog.get("readiness_status")
        or ""
    ).upper()

    if bool(controller.get("auto_rollback_required")):
        failures.append("MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_REQUIRED")
    if int(controller.get("invalid_effective_skip_count") or 0) > 0:
        failures.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
    if int(outbox.get("error_count") or 0) > 0 or int(
        outbox.get("dead_letter_count") or 0
    ) > 0:
        failures.append("PROJECTION_OUTBOX_ERROR_OR_DEAD_LETTER")
    if isinstance(latest_run, dict) and latest_run.get("status") == "FAIL":
        failures.append("LATEST_RECONCILE_FAIL")
    if backlog_status == "FAIL":
        failures.append("PROJECTION_OUTBOX_BACKLOG_FAIL")
    if int(routing.get("condition_event_candidate_ingest_executed_count") or 0) > 0:
        failures.append("CONDITION_EVENT_CANDIDATE_INGEST_IN_WORKER")
    if (
        not bool(controller.get("global_kill_switch"))
        and "MARKET_DATA_APPEND_ONLY_HEALTH_STALE"
        in set(controller.get("reason_codes") or [])
    ):
        failures.append("MARKET_DATA_APPEND_ONLY_HEALTH_STALE")
    failures.extend(_event_type_mode_violations(controller, routing))

    if operating_mode in {"OFF", "DRY_RUN"}:
        warnings.append(f"OPERATING_MODE_{operating_mode}")
    if bool(controller.get("global_kill_switch")):
        warnings.append("GLOBAL_KILL_SWITCH_ACTIVE")
    if int(controller.get("global_skip_budget_remaining") or 0) <= 0:
        warnings.append("GLOBAL_SKIP_BUDGET_EXHAUSTED")
    if backlog_status == "WARN":
        warnings.append("PROJECTION_OUTBOX_BACKLOG_WARN")
    if "MARKET_DATA_APPEND_ONLY_HEALTH_STALE" in set(
        controller.get("reason_codes") or []
    ):
        warnings.append("MARKET_DATA_APPEND_ONLY_HEALTH_STALE")
    if dashboard_controller and dashboard_controller.get("status") != controller.get("status"):
        warnings.append("DASHBOARD_CONTROLLER_STATUS_MISMATCH")

    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "operating_mode": operating_mode,
        "auto_rollback_required": bool(controller.get("auto_rollback_required")),
        "global_kill_switch": bool(controller.get("global_kill_switch")),
        "backlog_readiness_status": backlog_status,
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
    return {"raw": raw_path, "summary": summary_path}


def render_console_summary(report: dict[str, Any]) -> str:
    verdict = report["verdict"]
    controller = _data(report, "controller_status")
    return (
        "market_data append-only controller: "
        f"{verdict['status']} mode={controller.get('operating_mode')} "
        f"kill={controller.get('global_kill_switch')} "
        f"rollback={controller.get('auto_rollback_required')} "
        f"global_remaining={controller.get('global_skip_budget_remaining')} "
        f"backlog={controller.get('backlog_readiness_status')}"
    )


def render_markdown_summary(report: dict[str, Any]) -> str:
    verdict = report["verdict"]
    controller = _data(report, "controller_status")
    return "\n".join(
        [
            "# Market Data Append-Only Controller Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- operating_mode: `{controller.get('operating_mode')}`",
            f"- global_kill_switch: `{controller.get('global_kill_switch')}`",
            f"- effective_cutover_enabled: `{controller.get('effective_cutover_enabled')}`",
            f"- auto_rollback_required: `{controller.get('auto_rollback_required')}`",
            f"- allowed_event_types: `{controller.get('allowed_event_types')}`",
            f"- global_budget_remaining: `{controller.get('global_skip_budget_remaining')}`",
            f"- backlog_readiness_status: `{controller.get('backlog_readiness_status')}`",
            f"- latest_reconcile_status: `{controller.get('latest_reconcile_status')}`",
            f"- failures: `{verdict.get('failures', [])}`",
            f"- warnings: `{verdict.get('warnings', [])}`",
            "",
            "## Safety",
            "",
            "- Controller changes no config values.",
            "- Global kill switch and auto rollback gate precede effective skip.",
            "- LIVE_SIM/LIVE_REAL/order behavior is unchanged.",
            "- market_data 외 projection은 inline 유지 대상입니다.",
            "",
        ]
    )


def _data(report: dict[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key) or {}
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, dict) else {}


def _event_type_mode_violations(
    controller: Mapping[str, Any],
    routing: Mapping[str, Any],
) -> list[str]:
    allowed = set(str(v) for v in controller.get("allowed_event_types") or [])
    checks = {
        "price_tick": int(routing.get("effective_price_tick_skip_count") or 0),
        "tr_response": int(routing.get("tr_response_effective_skip_count") or 0),
        "condition_event": int(routing.get("condition_event_effective_skip_count") or 0),
    }
    return [
        f"EFFECTIVE_SKIP_NOT_ALLOWED_BY_MODE:{event_type}"
        for event_type, count in checks.items()
        if count > 0 and event_type not in allowed
    ]


if __name__ == "__main__":
    raise SystemExit(main())
