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
        description="Check the OBSERVE-only market-index TR bootstrap boundary."
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
    parser.add_argument("--expect-events", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_index_tr_bootstrap"),
    )
    args = parser.parse_args()
    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        expect_events=args.expect_events,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    expect_events: bool,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "market_index_tr_bootstrap,market_indexes,pipeline_summary",
            "timeout_budget_ms": "5000",
        }
    )
    report: dict[str, Any] = {
        "generated_at": _now(),
        "core_url": base_url,
        "expect_events": expect_events,
        "read_only_except_plan": True,
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
        "bootstrap_status": fetch_json(
            f"{base_url}/api/operator/market-index/tr-bootstrap/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "plan_only": fetch_json(
            f"{base_url}/api/operator/market-index/tr-bootstrap/run-once",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        ),
        "market_index_status": fetch_json(
            f"{base_url}/api/market-indexes/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "latest_reconcile": fetch_json(
            f"{base_url}/api/operator/market-index-projection-reconcile/latest",
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
        "bootstrap_status",
        "plan_only",
        "market_index_status",
        "latest_reconcile",
        "dashboard_snapshot",
        "command_status_after",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status_before")
    after = _data(report, "command_status_after")
    status = _data(report, "bootstrap_status")
    plan = _data(report, "plan_only")
    index_status = _data(report, "market_index_status")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_status = _mapping(dashboard.get("market_index_tr_bootstrap"))
    dashboard_summary = _mapping(
        _mapping(dashboard.get("pipeline_summary")).get(
            "market_index_tr_bootstrap"
        )
    )

    if core.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core.get("live_sim_allowed")) or bool(core.get("live_real_allowed")):
        failures.append("LIVE_TRADING_ALLOWED")
    if str(status.get("adapter_status") or "") != "IMPLEMENTED":
        failures.append("MARKET_INDEX_TR_BOOTSTRAP_ADAPTER_NOT_IMPLEMENTED")
    if status.get("nxt_is_not_valid_market_index_evidence") is not True:
        failures.append("NXT_MARKET_INDEX_EVIDENCE_GUARD_MISSING")

    bootstrap_status = str(status.get("status") or "")
    if bootstrap_status == "DISABLED":
        warnings.append("MARKET_INDEX_TR_BOOTSTRAP_DISABLED_SAFE_DEFAULT")
    elif bootstrap_status == "READY_KOA_PENDING":
        warnings.append("MARKET_INDEX_TR_BOOTSTRAP_KOA_STUDIO_PENDING")
    elif bootstrap_status != "READY":
        failures.append("MARKET_INDEX_TR_BOOTSTRAP_STATUS_UNKNOWN")

    if int(plan.get("command_count") or 0):
        failures.append("PLAN_ONLY_CREATED_COMMAND")
    if plan.get("status") not in {"DISABLED", "PLAN_ONLY"}:
        failures.append("PLAN_ONLY_STATUS_INVALID")
    if bool(plan.get("no_order_side_effects")) is not True:
        failures.append("PLAN_ONLY_ORDER_SAFETY_GUARD_MISSING")

    command_delta = _command_count(after) - _command_count(before)
    order_command_delta = int(after.get("order_command_count") or 0) - int(
        before.get("order_command_count") or 0
    )
    if command_delta:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")

    event_count = int(status.get("event_count") or 0)
    sample_count = int(status.get("sample_count") or 0)
    if report.get("expect_events") and event_count <= 0:
        failures.append("MARKET_INDEX_TR_BOOTSTRAP_EVENT_MISSING")
    if sample_count != event_count:
        failures.append("MARKET_INDEX_TR_BOOTSTRAP_SAMPLE_GAP")
    source_contract = _mapping(index_status.get("source_contract"))
    if source_contract.get("tr_bootstrap_adapter_status") != "IMPLEMENTED":
        failures.append("MARKET_INDEX_SOURCE_CONTRACT_NOT_UPDATED")
    if dashboard_status.get("status") != bootstrap_status:
        failures.append("DASHBOARD_BOOTSTRAP_STATUS_MISMATCH")
    if dashboard_summary.get("status") != bootstrap_status:
        failures.append("DASHBOARD_SUMMARY_BOOTSTRAP_STATUS_MISMATCH")

    verdict_status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict_status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "bootstrap_status": bootstrap_status,
        "event_count": event_count,
        "sample_count": sample_count,
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "read_only_except_plan": True,
        "no_order_side_effects": command_delta == 0 and order_command_delta == 0,
        "no_trading_side_effects": True,
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
    verdict = _mapping(report.get("verdict"))
    return "\n".join(
        [
            "# Market Index TR Bootstrap Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- bootstrap_status: `{verdict.get('bootstrap_status')}`",
            f"- event/sample: `{verdict.get('event_count')}/{verdict.get('sample_count')}`",
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "The run-once call is plan-only and never queues an order command.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "Market-index TR bootstrap: "
        f"{verdict.get('status')} status={verdict.get('bootstrap_status')} "
        f"events={verdict.get('event_count')} samples={verdict.get('sample_count')}"
    )


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = _mapping(report.get(key))
    data = payload.get("data")
    return dict(data) if isinstance(data, Mapping) else dict(payload)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _command_count(status: Mapping[str, Any]) -> int:
    counts = status.get("counts") or status.get("status_counts")
    if not isinstance(counts, Mapping):
        return 0
    return sum(int(value or 0) for value in counts.values())


if __name__ == "__main__":
    raise SystemExit(main())
