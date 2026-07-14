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
        description="Check PR-20 preparation or PR-21 market_scan limited cutover."
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
    parser.add_argument("--worker-limit", type=int, default=2)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--run-worker", action="store_true")
    parser.add_argument("--expect-dry-run-ready", action="store_true")
    parser.add_argument("--expect-effective-skip", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_scan_projection"),
    )
    args = parser.parse_args()
    report = run_market_scan_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        worker_limit=args.worker_limit,
        timeout_sec=args.timeout_sec,
        run_worker=args.run_worker,
        expect_dry_run_ready=args.expect_dry_run_ready,
        expect_effective_skip=args.expect_effective_skip,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_market_scan_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    worker_limit: int,
    timeout_sec: float,
    run_worker: bool,
    expect_dry_run_ready: bool,
    expect_effective_skip: bool = False,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    command_before = fetch_json(
        f"{base_url}/api/gateway/commands/status",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    worker_run = (
        fetch_json(
            f"{base_url}/api/operator/projection-outbox/run-once?"
            + urllib.parse.urlencode(
                {
                    **(
                        {}
                        if expect_effective_skip
                        else {"projection_name": "market_scan"}
                    ),
                    "limit": min(
                        max(int(worker_limit), 2 if expect_effective_skip else 1),
                        20,
                    ),
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
    scan_worker_run = (
        fetch_json(
            f"{base_url}/api/operator/projection-outbox/run-once?"
            + urllib.parse.urlencode(
                {
                    "projection_name": "market_scan",
                    "limit": min(max(int(worker_limit), 1), 20),
                    "apply_projection": "true",
                    "live_safe": "true",
                }
            ),
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )
        if run_worker and expect_effective_skip
        else {"ok": True, "data": {"status": "NOT_RUN"}}
    )
    reconcile_run = fetch_json(
        f"{base_url}/api/operator/market-scan-projection-reconcile/run-once?"
        + urllib.parse.urlencode(
            {"limit": limit, "persist": "true", "live_safe": "true"}
        ),
        token=token,
        method="POST",
        timeout_sec=timeout_sec,
    )
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": (
                "market_scan,projection_outbox,"
                "market_scan_projection_reconcile,"
                "market_scan_append_only_routing,errors"
            ),
            "timeout_budget_ms": "5000",
        }
    )
    report = {
        "generated_at": _now(),
        "core_url": base_url,
        "run_worker": bool(run_worker),
        "expect_dry_run_ready": bool(expect_dry_run_ready),
        "expect_effective_skip": bool(expect_effective_skip),
        "core_status": fetch_json(
            f"{base_url}/api/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "command_status_before": command_before,
        "worker_run": worker_run,
        "scan_worker_run": scan_worker_run,
        "reconcile_run": reconcile_run,
        "latest_reconcile": fetch_json(
            f"{base_url}/api/operator/market-scan-projection-reconcile/latest",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "routing_status": fetch_json(
            f"{base_url}/api/operator/market-scan-append-only-routing/status",
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
            f"{base_url}/api/dashboard/snapshot?{dashboard_query}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "command_status_after": fetch_json(
            f"{base_url}/api/gateway/commands/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in (
        "core_status",
        "command_status_before",
        "worker_run",
        "reconcile_run",
        "latest_reconcile",
        "routing_status",
        "projection_outbox",
        "dashboard_snapshot",
        "command_status_after",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")
    if report.get("run_worker") and report.get("expect_effective_skip"):
        payload = report.get("scan_worker_run")
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append("SCAN_WORKER_RUN_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status_before")
    after = _data(report, "command_status_after")
    worker = _data(report, "worker_run")
    scan_worker = _data(report, "scan_worker_run")
    reconcile = _data(report, "reconcile_run")
    routing = _data(report, "routing_status")
    outbox = _data(report, "projection_outbox")
    dashboard = _data(report, "dashboard_snapshot")

    if core.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core.get("live_sim_allowed")):
        failures.append("LIVE_SIM_ALLOWED")
    if bool(core.get("live_real_allowed")):
        failures.append("LIVE_REAL_ALLOWED")
    if bool(before.get("order_commands_allowed")) or bool(
        after.get("order_commands_allowed")
    ):
        failures.append("ORDER_COMMANDS_ALLOWED")

    command_delta = _command_count(after) - _command_count(before)
    order_command_delta = int(after.get("order_command_count") or 0) - int(
        before.get("order_command_count") or 0
    )
    if command_delta:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")

    reconcile_status = str(reconcile.get("status") or "").upper()
    if reconcile_status == "FAIL":
        failures.append("MARKET_SCAN_RECONCILE_FAIL")
    elif reconcile_status != "PASS":
        warnings.append("MARKET_SCAN_RECONCILE_NOT_PASS")
    if reconcile_status == "PASS" and not bool(reconcile.get("append_only_ready")):
        failures.append("MARKET_SCAN_APPEND_ONLY_NOT_READY")

    effective_skip_count = int(routing.get("effective_skip_inline_count") or 0)
    would_skip_count = int(routing.get("would_skip_inline_count") or 0)
    expect_effective_skip = bool(report.get("expect_effective_skip"))
    legacy_guard = bool(routing.get("effective_skip_disabled_in_pr20", True))
    if expect_effective_skip:
        if effective_skip_count <= 0:
            failures.append("MARKET_SCAN_EFFECTIVE_SKIP_EVIDENCE_MISSING")
        if not bool(routing.get("cutover_enabled")):
            failures.append("MARKET_SCAN_CUTOVER_DISABLED")
        if bool(routing.get("global_kill_switch", True)):
            failures.append("MARKET_SCAN_GLOBAL_KILL_SWITCH_ENABLED")
        if legacy_guard:
            failures.append("MARKET_SCAN_PR20_GUARD_ENABLED")
        if str(routing.get("controller_status") or "").upper() != "PASS":
            failures.append("MARKET_SCAN_CONTROLLER_NOT_PASS")
        if bool(routing.get("rollback_required")):
            failures.append("MARKET_SCAN_ROLLBACK_REQUIRED")
        health = _mapping(routing.get("effective_skip_health"))
        if any(int(value or 0) for value in health.values()):
            failures.append("MARKET_SCAN_EFFECTIVE_SKIP_WORKER_CLOSURE_FAILED")
    elif legacy_guard and effective_skip_count:
        failures.append("MARKET_SCAN_EFFECTIVE_SKIP_FORBIDDEN_IN_PR20")
    elif effective_skip_count:
        warnings.append("MARKET_SCAN_HISTORICAL_EFFECTIVE_SKIP_PRESENT")
    if report.get("expect_dry_run_ready") and would_skip_count <= 0:
        failures.append("MARKET_SCAN_DRY_RUN_READY_EVIDENCE_MISSING")

    scan_outbox = _mapping(outbox.get("by_projection_name")).get("market_scan")
    scan_outbox = _mapping(scan_outbox)
    if int(scan_outbox.get("error_count") or 0):
        failures.append("MARKET_SCAN_OUTBOX_ERROR_PRESENT")
    if int(scan_outbox.get("dead_letter_count") or 0):
        failures.append("MARKET_SCAN_OUTBOX_DEAD_LETTER_PRESENT")

    if report.get("run_worker"):
        closure_worker = scan_worker if expect_effective_skip else worker
        if not bool(closure_worker.get("market_scan_apply_enabled")):
            failures.append("MARKET_SCAN_WORKER_APPLY_DISABLED")
        if int(worker.get("error_count") or 0) or int(
            scan_worker.get("error_count") or 0
        ):
            failures.append("MARKET_SCAN_WORKER_ERROR")
        mutated = {
            str(value)
            for value in (
                list(worker.get("mutated_projection_names") or [])
                + list(scan_worker.get("mutated_projection_names") or [])
            )
        }
        allowed_mutations = {"market_data", "market_scan"}
        if not mutated <= allowed_mutations:
            failures.append("MARKET_SCAN_WORKER_MUTATION_SCOPE_INVALID")
        if (
            expect_effective_skip
            and (
                int(worker.get("applied_by_worker_count") or 0)
                + int(scan_worker.get("applied_by_worker_count") or 0)
            )
            > 0
            and "market_scan" not in mutated
        ):
            failures.append("MARKET_SCAN_WORKER_MUTATION_EVIDENCE_MISSING")

    if "market_scan_projection_reconcile" not in dashboard:
        failures.append("DASHBOARD_MARKET_SCAN_RECONCILE_MISSING")
    if "market_scan_append_only_routing" not in dashboard:
        failures.append("DASHBOARD_MARKET_SCAN_ROUTING_MISSING")

    status = "FAIL" if failures else ("WARN" if warnings else "PASS")
    return {
        "status": status,
        "failures": list(dict.fromkeys(failures)),
        "warnings": list(dict.fromkeys(warnings)),
        "reconcile_status": reconcile_status,
        "would_skip_inline_count": would_skip_count,
        "effective_skip_inline_count": effective_skip_count,
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "candidate_ingest_executed_count": 0,
        "no_trading_side_effects": not failures,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir / stamp
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(_render_markdown(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        f"market_scan projection check: {verdict.get('status', 'UNKNOWN')} "
        f"reconcile={verdict.get('reconcile_status')} "
        f"would_skip={verdict.get('would_skip_inline_count', 0)} "
        f"effective_skip={verdict.get('effective_skip_inline_count', 0)}"
    )


def _render_markdown(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    failures = verdict.get("failures") or []
    warnings = verdict.get("warnings") or []
    return "\n".join(
        (
            "# Market Scan Projection Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status', 'UNKNOWN')}`",
            f"- reconcile_status: `{verdict.get('reconcile_status')}`",
            f"- would_skip_inline_count: `{verdict.get('would_skip_inline_count', 0)}`",
            f"- effective_skip_inline_count: `{verdict.get('effective_skip_inline_count', 0)}`",
            f"- command_count_delta: `{verdict.get('command_count_delta', 0)}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta', 0)}`",
            "- candidate_ingest_executed_count: "
            f"`{verdict.get('candidate_ingest_executed_count', 0)}`",
            f"- failures: `{json.dumps(failures, ensure_ascii=False)}`",
            f"- warnings: `{json.dumps(warnings, ensure_ascii=False)}`",
            "",
            "PR-20 keeps effective skip at zero. PR-21 requires explicit cutover, "
            "budget, worker closure, and rollback evidence.",
        )
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    data = payload.get("data")
    return dict(data) if isinstance(data, Mapping) else dict(payload)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _command_count(payload: Mapping[str, Any]) -> int:
    counts = payload.get("counts")
    if not isinstance(counts, Mapping):
        return 0
    return sum(int(value or 0) for value in counts.values())


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
