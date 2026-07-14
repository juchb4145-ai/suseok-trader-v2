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
        description="Check PR-14 market_reference limited-cutover readiness."
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
    parser.add_argument("--run-worker", action="store_true")
    parser.add_argument("--expect-effective-skip", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_reference_projection"),
    )
    args = parser.parse_args()

    report = run_market_reference_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
        run_worker=args.run_worker,
        expect_effective_skip=args.expect_effective_skip,
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_market_reference_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    timeout_sec: float,
    out_dir: Path,
    run_worker: bool = False,
    expect_effective_skip: bool = False,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_params = {
        "sections": (
            "gateway,market_data,market_reference,projection_outbox,"
            "pipeline_summary,errors"
        ),
        "detail": "summary",
        "limit": "20",
        "fast": "true",
        "timeout_budget_ms": "5000",
    }
    worker_run = (
        fetch_json(
            f"{base_url}/api/operator/projection-outbox/run-once?"
            + urllib.parse.urlencode(
                {
                    "projection_name": "market_reference",
                    "limit": 1,
                    "apply_projection": "true",
                    "live_safe": "true",
                }
            ),
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )
        if run_worker
        else {"ok": True, "data": {"status": "NOT_RUN"}}
    )
    reconcile_run = fetch_json(
        f"{base_url}/api/operator/market-reference-projection-reconcile/run-once"
        f"?{urllib.parse.urlencode({'limit': limit, 'persist': 'true', 'live_safe': 'true'})}",
        token=token,
        method="POST",
        timeout_sec=timeout_sec,
    )
    report = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "core_url": base_url,
        "run_worker": bool(run_worker),
        "expect_effective_skip": bool(expect_effective_skip),
        "worker_run": worker_run,
        "reconcile_run": reconcile_run,
        "latest_reconcile": fetch_json(
            f"{base_url}/api/operator/market-reference-projection-reconcile/latest",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "routing_status": fetch_json(
            f"{base_url}/api/operator/market-reference-append-only-routing/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "market_reference_status": fetch_json(
            f"{base_url}/api/operator/market-reference/status",
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
        "worker_run",
        "reconcile_run",
        "latest_reconcile",
        "routing_status",
        "market_reference_status",
        "projection_outbox",
        "dashboard_snapshot",
    ):
        payload = report.get(key) or {}
        if not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    reconcile_run = _data(report, "reconcile_run")
    latest_reconcile = _data(report, "latest_reconcile")
    routing = _data(report, "routing_status")
    reference_status = _data(report, "market_reference_status")
    projection_outbox = _data(report, "projection_outbox")
    dashboard = _data(report, "dashboard_snapshot")
    latest_run = (
        latest_reconcile.get("latest_run")
        if isinstance(latest_reconcile, Mapping)
        else None
    )
    run_payload = latest_run if isinstance(latest_run, Mapping) else reconcile_run

    membership_count = int(
        reference_status.get("membership_count")
        or run_payload.get("stored_membership_count")
        or 0
    )
    missing_membership_count = int(
        reference_status.get("missing_membership_count")
        or run_payload.get("missing_membership_count")
        or 0
    )
    outbox_error_count = int(
        run_payload.get("outbox_error_count")
        or projection_outbox.get("by_projection_name", {})
        .get("market_reference", {})
        .get("error_count", 0)
        or 0
    )
    outbox_dead_letter_count = int(
        run_payload.get("outbox_dead_letter_count")
        or projection_outbox.get("by_projection_name", {})
        .get("market_reference", {})
        .get("dead_letter_count", 0)
        or 0
    )
    effective_skip_count = int(
        routing.get("effective_skip_inline_count")
        or reference_status.get("effective_skip_inline_count")
        or 0
    )
    reconcile_status = str(run_payload.get("status") or "").upper()
    latest_outbox_job = reference_status.get("latest_outbox_job")
    latest_outbox_job = (
        latest_outbox_job if isinstance(latest_outbox_job, Mapping) else {}
    )
    latest_outbox_metadata = latest_outbox_job.get("metadata")
    latest_outbox_metadata = (
        latest_outbox_metadata
        if isinstance(latest_outbox_metadata, Mapping)
        else {}
    )
    worker_evidence = latest_outbox_metadata.get("last_worker_evidence")
    worker_evidence = worker_evidence if isinstance(worker_evidence, Mapping) else {}
    worker_apply_mode = str(worker_evidence.get("apply_mode") or "")
    worker_apply_result = str(worker_evidence.get("apply_result") or "")
    worker_no_trading_side_effects = bool(
        worker_evidence.get("no_trading_side_effects")
    )
    controller_status = str(routing.get("status") or "").upper()
    rollback_required = bool(routing.get("rollback_required"))
    skip_budget_limit = int(routing.get("skip_budget_limit") or 0)
    skip_budget_used = int(routing.get("skip_budget_used_current_minute") or 0)
    effective_skip_health = routing.get("effective_skip_health")
    effective_skip_health = (
        effective_skip_health if isinstance(effective_skip_health, Mapping) else {}
    )
    latest_decision = routing.get("latest_decision")
    latest_decision = latest_decision if isinstance(latest_decision, Mapping) else {}
    latest_decision_evidence = latest_decision.get("evidence")
    latest_decision_evidence = (
        latest_decision_evidence
        if isinstance(latest_decision_evidence, Mapping)
        else {}
    )
    latest_decision_event_id = str(latest_decision.get("event_id") or "")
    latest_decision_effective_skip = bool(
        latest_decision.get("effective_skip_inline")
    )
    latest_decision_budget_limit = int(
        latest_decision_evidence.get("skip_budget_limit") or 0
    )
    latest_decision_budget_used = int(
        latest_decision_evidence.get("skip_budget_used") or 0
    )
    latest_market_symbols_event_id = str(
        reference_status.get("latest_market_symbols_event_id") or ""
    )
    dashboard_gateway = dashboard.get("gateway")
    dashboard_gateway = (
        dashboard_gateway if isinstance(dashboard_gateway, Mapping) else {}
    )
    realtime_exchange = str(dashboard_gateway.get("realtime_exchange") or "").upper()
    kiwoom_logged_in = bool(dashboard_gateway.get("kiwoom_logged_in"))
    expect_effective_skip = bool(report.get("expect_effective_skip"))

    if membership_count <= 0:
        failures.append("MARKET_REFERENCE_MEMBERSHIP_COUNT_ZERO")
    if missing_membership_count > 0:
        failures.append("MARKET_REFERENCE_MEMBERSHIP_MISSING")
    if outbox_error_count > 0 or outbox_dead_letter_count > 0:
        failures.append("MARKET_REFERENCE_OUTBOX_ERROR_OR_DEAD_LETTER")
    if reconcile_status == "FAIL":
        failures.append("MARKET_REFERENCE_RECONCILE_FAIL")
    if effective_skip_count > 0:
        if not bool(routing.get("cutover_enabled")):
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_WITH_CUTOVER_DISABLED")
        if bool(routing.get("global_kill_switch")):
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_WITH_KILL_SWITCH")
        if not bool(routing.get("worker_apply_enabled")):
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_WITH_WORKER_DISABLED")
        if rollback_required:
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_ROLLBACK_REQUIRED")
        if int(effective_skip_health.get("pending_worker_count") or 0) > 0:
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_PENDING_WORKER")
        if int(effective_skip_health.get("worker_error_count") or 0) > 0:
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_WORKER_ERROR")
        if int(
            effective_skip_health.get("worker_apply_evidence_missing_count") or 0
        ) > 0:
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_WORKER_EVIDENCE_MISSING")
        if int(effective_skip_health.get("artifact_missing_count") or 0) > 0:
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_ARTIFACT_MISSING")
    if expect_effective_skip and effective_skip_count <= 0:
        failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_NOT_OBSERVED")
    if expect_effective_skip and controller_status != "PASS":
        failures.append("MARKET_REFERENCE_CONTROLLER_NOT_PASS")
    if expect_effective_skip and skip_budget_limit <= 0:
        failures.append("MARKET_REFERENCE_SKIP_BUDGET_DISABLED")
    if expect_effective_skip and not latest_decision_effective_skip:
        failures.append("MARKET_REFERENCE_LATEST_DECISION_NOT_EFFECTIVE_SKIP")
    if expect_effective_skip and latest_decision_budget_limit != 1:
        failures.append("MARKET_REFERENCE_LATEST_DECISION_BUDGET_NOT_ONE")
    if expect_effective_skip and latest_decision_budget_used != 1:
        failures.append("MARKET_REFERENCE_LATEST_DECISION_BUDGET_NOT_CONSUMED")
    if expect_effective_skip and not latest_market_symbols_event_id:
        failures.append("MARKET_REFERENCE_LATEST_MARKET_SYMBOLS_EVENT_MISSING")
    if (
        expect_effective_skip
        and latest_market_symbols_event_id
        and latest_decision_event_id != latest_market_symbols_event_id
    ):
        failures.append("MARKET_REFERENCE_LATEST_DECISION_EVENT_MISMATCH")
    if expect_effective_skip and realtime_exchange not in {"KRX", "NXT"}:
        failures.append("MARKET_REFERENCE_GATEWAY_VENUE_UNKNOWN")
    if expect_effective_skip and not kiwoom_logged_in:
        failures.append("MARKET_REFERENCE_GATEWAY_NOT_LOGGED_IN")
    if _terminal_artifact_missing(run_payload):
        failures.append("MARKET_REFERENCE_TERMINAL_ARTIFACT_MISSING")
    if not worker_evidence:
        warnings.append("MARKET_REFERENCE_WORKER_EVIDENCE_MISSING")
    else:
        if worker_apply_mode != "MARKET_REFERENCE_APPLY":
            failures.append("MARKET_REFERENCE_WORKER_APPLY_MODE_INVALID")
        if worker_apply_result not in {"APPLIED_BY_VERIFY", "APPLIED_BY_WORKER"}:
            failures.append("MARKET_REFERENCE_WORKER_APPLY_RESULT_INVALID")
        if expect_effective_skip and worker_apply_result != "APPLIED_BY_WORKER":
            failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_NOT_APPLIED_BY_WORKER")
        if not worker_no_trading_side_effects:
            failures.append("MARKET_REFERENCE_WORKER_TRADING_SIDE_EFFECT_GUARD_MISSING")

    if reconcile_status == "WARN":
        warnings.append("MARKET_REFERENCE_RECONCILE_WARN")
    if not bool(routing.get("dry_run_enabled")):
        warnings.append("MARKET_REFERENCE_DRY_RUN_DISABLED")
    if int(run_payload.get("outbox_pending_count") or 0) > 0:
        warnings.append("MARKET_REFERENCE_OUTBOX_PENDING_WITHIN_SLA")
    if not bool(run_payload.get("append_only_ready")):
        warnings.append("MARKET_REFERENCE_APPEND_ONLY_NOT_READY")
    dashboard_reference = (
        dashboard.get("market_reference") if isinstance(dashboard, Mapping) else None
    )
    if isinstance(dashboard_reference, Mapping) and int(
        dashboard_reference.get("effective_skip_inline_count") or 0
    ) != effective_skip_count:
        failures.append("DASHBOARD_MARKET_REFERENCE_EFFECTIVE_SKIP_MISMATCH")

    worker_evidence_missing = "MARKET_REFERENCE_WORKER_EVIDENCE_MISSING" in warnings
    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_pr14": bool(failures or worker_evidence_missing),
        "block_next_pr": bool(failures or worker_evidence_missing),
        "membership_count": membership_count,
        "missing_membership_count": missing_membership_count,
        "outbox_error_count": outbox_error_count,
        "outbox_dead_letter_count": outbox_dead_letter_count,
        "effective_skip_inline_count": effective_skip_count,
        "controller_status": controller_status or None,
        "rollback_required": rollback_required,
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used_current_minute": skip_budget_used,
        "latest_decision_event_id": latest_decision_event_id or None,
        "latest_decision_effective_skip": latest_decision_effective_skip,
        "latest_decision_budget_limit": latest_decision_budget_limit,
        "latest_decision_budget_used": latest_decision_budget_used,
        "latest_market_symbols_event_id": latest_market_symbols_event_id or None,
        "realtime_exchange": realtime_exchange or None,
        "kiwoom_logged_in": kiwoom_logged_in,
        "expect_effective_skip": expect_effective_skip,
        "reconcile_status": reconcile_status,
        "worker_apply_mode": worker_apply_mode or None,
        "worker_apply_result": worker_apply_result or None,
        "worker_no_trading_side_effects": worker_no_trading_side_effects,
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


def render_console_summary(report: dict[str, Any]) -> str:
    verdict = report["verdict"]
    return (
        "market_reference projection: "
        f"{verdict['status']} reconcile={verdict.get('reconcile_status')} "
        f"membership={verdict.get('membership_count')} "
        f"missing={verdict.get('missing_membership_count')} "
        f"effective_skip={verdict.get('effective_skip_inline_count')}"
    )


def render_markdown_summary(report: dict[str, Any]) -> str:
    verdict = report["verdict"]
    return "\n".join(
        [
            "# Market Reference Projection Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- reconcile_status: `{verdict.get('reconcile_status')}`",
            f"- membership_count: `{verdict.get('membership_count')}`",
            f"- missing_membership_count: `{verdict.get('missing_membership_count')}`",
            f"- outbox_error_count: `{verdict.get('outbox_error_count')}`",
            f"- outbox_dead_letter_count: `{verdict.get('outbox_dead_letter_count')}`",
            f"- effective_skip_inline_count: `{verdict.get('effective_skip_inline_count')}`",
            f"- controller_status: `{verdict.get('controller_status')}`",
            f"- rollback_required: `{verdict.get('rollback_required')}`",
            f"- skip_budget_limit: `{verdict.get('skip_budget_limit')}`",
            (
                "- skip_budget_used_current_minute: "
                f"`{verdict.get('skip_budget_used_current_minute')}`"
            ),
            f"- latest_decision_budget_limit: `{verdict.get('latest_decision_budget_limit')}`",
            f"- latest_decision_budget_used: `{verdict.get('latest_decision_budget_used')}`",
            f"- realtime_exchange: `{verdict.get('realtime_exchange')}`",
            f"- kiwoom_logged_in: `{verdict.get('kiwoom_logged_in')}`",
            f"- worker_apply_mode: `{verdict.get('worker_apply_mode')}`",
            f"- worker_apply_result: `{verdict.get('worker_apply_result')}`",
            f"- worker_no_trading_side_effects: `{verdict.get('worker_no_trading_side_effects')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "PR-14 allows only guarded, budgeted market_reference effective skips.",
            "Rollback keeps market_symbols inline projection available.",
        ]
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    if "data" in payload and isinstance(payload["data"], Mapping):
        return dict(payload["data"])
    return dict(payload)


def _terminal_artifact_missing(payload: Mapping[str, Any]) -> bool:
    reason_codes = {str(value) for value in payload.get("reason_codes") or []}
    return "MARKET_REFERENCE_ARTIFACT_MISSING_AFTER_TERMINAL_OUTBOX" in reason_codes


if __name__ == "__main__":
    raise SystemExit(main())
