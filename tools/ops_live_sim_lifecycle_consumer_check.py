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

from tools.ops_market_data_tr_response_side_effect_check import fetch_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check LIVE_SIM lifecycle durable consumer without routing orders."
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
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "live_sim_lifecycle_consumer"),
    )
    args = parser.parse_args()
    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "live_sim_lifecycle_consumer,pipeline_summary",
            "timeout_budget_ms": "5000",
        }
    )
    report = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "core_url": base_url,
        "core_status": fetch_json(
            f"{base_url}/api/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "command_status_before": fetch_json(
            f"{base_url}/api/gateway/commands/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "consumer_status": fetch_json(
            f"{base_url}/api/operator/live-sim/lifecycle-consumer/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "inbox": fetch_json(
            f"{base_url}/api/operator/live-sim/lifecycle-consumer/inbox?limit=100",
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
        "consumer_status",
        "inbox",
        "dashboard_snapshot",
        "command_status_after",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status_before")
    after = _data(report, "command_status_after")
    status = _data(report, "consumer_status")
    inbox = _data(report, "inbox")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_status = dashboard.get("live_sim_lifecycle_consumer")
    dashboard_status = dashboard_status if isinstance(dashboard_status, Mapping) else {}

    if core.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core.get("live_sim_allowed")):
        failures.append("LIVE_SIM_ALLOWED")
    if bool(core.get("live_real_allowed")):
        failures.append("LIVE_REAL_ALLOWED")
    if bool(before.get("order_commands_allowed")) or bool(after.get("order_commands_allowed")):
        failures.append("ORDER_COMMANDS_ALLOWED")
    command_delta = _command_count(after) - _command_count(before)
    order_command_delta = int(after.get("order_command_count") or 0) - int(
        before.get("order_command_count") or 0
    )
    if command_delta:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")

    for field in (
        "dead_letter_count",
        "stale_processing_count",
        "missing_inbox_count",
        "applied_without_result_count",
    ):
        if int(status.get(field) or 0):
            failures.append(f"LIFECYCLE_{field.upper()}")
    if str(status.get("status")) == "FAIL":
        failures.append("LIFECYCLE_CONSUMER_STATUS_FAIL")
    if int(status.get("pending_count") or 0) or int(status.get("processing_count") or 0):
        warnings.append("LIFECYCLE_BACKLOG_PRESENT")
    if not bool(status.get("consumer_enabled")):
        warnings.append("LIFECYCLE_CONSUMER_DISABLED_PREPARATION")
    if not bool(status.get("worker_enabled")):
        warnings.append("LIFECYCLE_WORKER_DISABLED_PREPARATION")
    cutover_enabled = bool(status.get("cutover_enabled"))
    if cutover_enabled:
        if bool(status.get("global_kill_switch")):
            failures.append("LIFECYCLE_CUTOVER_KILL_SWITCH_ON")
        worker_health = status.get("worker_health")
        worker_health = worker_health if isinstance(worker_health, Mapping) else {}
        if not bool(worker_health.get("healthy")):
            failures.append("LIFECYCLE_CUTOVER_WORKER_UNHEALTHY")
        if int(status.get("effective_defer_count") or 0) <= 0:
            warnings.append("LIFECYCLE_CUTOVER_NOT_EXERCISED")
    if not bool(inbox.get("read_only")):
        failures.append("LIFECYCLE_INBOX_NOT_READ_ONLY")
    if not dashboard_status:
        failures.append("DASHBOARD_LIFECYCLE_CONSUMER_MISSING")
    elif int(dashboard_status.get("total_count") or 0) != int(
        status.get("total_count") or 0
    ):
        failures.append("DASHBOARD_LIFECYCLE_CONSUMER_MISMATCH")

    verdict_status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict_status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "inbox_total_count": int(status.get("total_count") or 0),
        "inbox_applied_count": int(status.get("applied_count") or 0),
        "inbox_dead_letter_count": int(status.get("dead_letter_count") or 0),
        "cutover_enabled": cutover_enabled,
        "effective_defer_count": int(status.get("effective_defer_count") or 0),
        "read_only_check": True,
        "no_order_commands_created": True,
        "live_real_allowed": False,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return "\n".join(
        [
            "# LIVE_SIM Lifecycle Consumer Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- inbox_total_count: `{verdict.get('inbox_total_count')}`",
            f"- inbox_applied_count: `{verdict.get('inbox_applied_count')}`",
            f"- inbox_dead_letter_count: `{verdict.get('inbox_dead_letter_count')}`",
            f"- cutover_enabled: `{verdict.get('cutover_enabled')}`",
            f"- effective_defer_count: `{verdict.get('effective_defer_count')}`",
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "This check does not poll, queue, or send broker order commands.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return (
        "LIVE_SIM lifecycle consumer: "
        f"{verdict.get('status')} total={verdict.get('inbox_total_count')} "
        f"applied={verdict.get('inbox_applied_count')} "
        f"dead_letter={verdict.get('inbox_dead_letter_count')}"
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    data = payload.get("data")
    return dict(data) if isinstance(data, Mapping) else dict(payload)


def _command_count(status: Mapping[str, Any]) -> int:
    counts = status.get("counts")
    if not isinstance(counts, Mapping):
        return 0
    return sum(int(value or 0) for value in counts.values())


if __name__ == "__main__":
    raise SystemExit(main())
