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
        description="Check LIVE_SIM order-plan column, backfill, and uniqueness health."
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
        default=str(ROOT_DIR / "reports" / "live_sim_order_plan_uniqueness"),
    )
    args = parser.parse_args()

    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] == "PASS" else 2


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
            "sections": "live_sim_order_plan_uniqueness,pipeline_summary",
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
        "command_status": fetch_json(
            f"{base_url}/api/gateway/commands/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "uniqueness_status": fetch_json(
            f"{base_url}/api/operator/live-sim/order-plan-uniqueness/status",
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
    for key in (
        "core_status",
        "command_status",
        "command_status_after",
        "uniqueness_status",
        "dashboard_snapshot",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core_status = _data(report, "core_status")
    command_status = _data(report, "command_status")
    command_status_after = _data(report, "command_status_after")
    status = _data(report, "uniqueness_status")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_status = dashboard.get("live_sim_order_plan_uniqueness")
    dashboard_status = (
        dashboard_status if isinstance(dashboard_status, Mapping) else {}
    )
    if core_status.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core_status.get("live_sim_allowed")):
        failures.append("LIVE_SIM_ALLOWED")
    if bool(core_status.get("live_real_allowed")):
        failures.append("LIVE_REAL_ALLOWED")
    if bool(command_status.get("order_commands_allowed")):
        failures.append("ORDER_COMMANDS_ALLOWED")
    if bool(command_status_after.get("order_commands_allowed")):
        failures.append("ORDER_COMMANDS_ALLOWED_AFTER_CHECK")
    command_count_delta = _command_count(command_status_after) - _command_count(
        command_status
    )
    order_command_count_delta = int(
        command_status_after.get("order_command_count") or 0
    ) - int(command_status.get("order_command_count") or 0)
    if command_count_delta != 0:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_count_delta != 0:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")
    if str(status.get("status") or "") != "PASS":
        failures.append("ORDER_PLAN_UNIQUENESS_NOT_PASS")
    if not bool(status.get("column_exists")):
        failures.append("ORDER_PLAN_ID_COLUMN_MISSING")
    if not (
        bool(status.get("unique_index_exists"))
        and bool(status.get("unique_index_is_unique"))
        and bool(status.get("unique_index_is_partial"))
    ):
        failures.append("ORDER_PLAN_UNIQUE_INDEX_INVALID")
    for field in (
        "duplicate_group_count",
        "mismatch_count",
        "missing_backfill_count",
        "invalid_evidence_order_plan_id_count",
    ):
        if int(status.get(field) or 0) > 0:
            failures.append(f"ORDER_PLAN_{field.upper()}")
    if status.get("lookup_strategy") != "DIRECT_ORDER_PLAN_ID_INDEX_LOOKUP":
        failures.append("ORDER_PLAN_DIRECT_LOOKUP_NOT_ACTIVE")
    if not dashboard_status:
        failures.append("DASHBOARD_ORDER_PLAN_UNIQUENESS_MISSING")
    elif dashboard_status.get("status") != status.get("status"):
        failures.append("DASHBOARD_ORDER_PLAN_UNIQUENESS_MISMATCH")

    return {
        "status": "FAIL" if failures else "PASS",
        "failures": sorted(set(failures)),
        "block_next_pr": bool(failures),
        "intent_count": int(status.get("intent_count") or 0),
        "order_plan_intent_count": int(status.get("order_plan_intent_count") or 0),
        "duplicate_group_count": int(status.get("duplicate_group_count") or 0),
        "order_command_count": int(
            command_status_after.get("order_command_count") or 0
        ),
        "command_count_delta": command_count_delta,
        "order_command_count_delta": order_command_count_delta,
        "no_trading_side_effects": bool(
            status.get("no_trading_side_effects", True)
        ),
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
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return "\n".join(
        [
            "# LIVE_SIM Order-Plan Uniqueness Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- intent_count: `{verdict.get('intent_count')}`",
            f"- order_plan_intent_count: `{verdict.get('order_plan_intent_count')}`",
            f"- duplicate_group_count: `{verdict.get('duplicate_group_count')}`",
            f"- order_command_count: `{verdict.get('order_command_count')}`",
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            (
                "- order_command_count_delta: "
                f"`{verdict.get('order_command_count_delta')}`"
            ),
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            "",
            "This check is read-only and does not create intents or queue commands.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return (
        "LIVE_SIM order-plan uniqueness: "
        f"{verdict.get('status')} intents={verdict.get('intent_count')} "
        f"order_plans={verdict.get('order_plan_intent_count')} "
        f"duplicates={verdict.get('duplicate_group_count')}"
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    data = payload.get("data")
    if isinstance(data, Mapping):
        return dict(data)
    return dict(payload)


def _command_count(status: Mapping[str, Any]) -> int:
    counts = status.get("counts")
    if not isinstance(counts, Mapping):
        return 0
    return sum(int(value or 0) for value in counts.values())


if __name__ == "__main__":
    raise SystemExit(main())
