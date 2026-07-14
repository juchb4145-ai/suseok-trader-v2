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
        description="Check durable order broker-boundary state without routing orders."
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
        default=str(ROOT_DIR / "reports" / "order_broker_boundary"),
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
            "sections": "order_broker_boundaries,pipeline_summary",
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
        "boundary_status": fetch_json(
            f"{base_url}/api/operator/gateway/order-broker-boundaries/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "boundary_rows": fetch_json(
            f"{base_url}/api/operator/gateway/order-broker-boundaries?limit=100",
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
        "command_status",
        "command_status_after",
        "boundary_status",
        "boundary_rows",
        "dashboard_snapshot",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core_status = _data(report, "core_status")
    command_status = _data(report, "command_status")
    command_status_after = _data(report, "command_status_after")
    status = _data(report, "boundary_status")
    rows = _data(report, "boundary_rows")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_status = dashboard.get("order_broker_boundaries")
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

    boundary_health = str(status.get("status") or "")
    if boundary_health == "FAIL" or boundary_health not in {"PASS", "WARN"}:
        failures.append("ORDER_BROKER_BOUNDARY_STATUS_FAIL")
    if not bool(status.get("table_exists")):
        failures.append("ORDER_BROKER_BOUNDARY_TABLE_MISSING")
    if not bool(status.get("required_indexes_present")):
        failures.append("ORDER_BROKER_BOUNDARY_INDEX_MISSING")
    for field in (
        "missing_boundary_count",
        "durable_pre_ack_gap_count",
        "duplicate_idempotency_count",
        "command_state_mismatch_count",
    ):
        if int(status.get(field) or 0) > 0:
            failures.append(f"ORDER_BOUNDARY_{field.upper()}")

    unconfirmed_count = int(status.get("unconfirmed_count") or 0)
    if unconfirmed_count > 0:
        warnings.append("UNCONFIRMED_ORDER_BOUNDARY_REQUIRES_RECONCILE")
        if not bool(status.get("block_new_order_routing")):
            failures.append("UNCONFIRMED_ORDER_BOUNDARY_NOT_BLOCKING")
    if not bool(status.get("no_order_side_effects", True)):
        failures.append("ORDER_SIDE_EFFECT_FLAG_FALSE")
    if not bool(status.get("no_trading_side_effects", True)):
        failures.append("TRADING_SIDE_EFFECT_FLAG_FALSE")
    if not bool(rows.get("read_only")):
        failures.append("ORDER_BOUNDARY_LIST_NOT_READ_ONLY")
    if not dashboard_status:
        failures.append("DASHBOARD_ORDER_BROKER_BOUNDARY_MISSING")
    elif dashboard_status.get("status") != status.get("status"):
        failures.append("DASHBOARD_ORDER_BROKER_BOUNDARY_MISMATCH")

    verdict_status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict_status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "block_new_order_routing": bool(status.get("block_new_order_routing")),
        "boundary_count": int(status.get("boundary_count") or 0),
        "durable_pre_ack_count": int(status.get("durable_pre_ack_count") or 0),
        "unconfirmed_count": unconfirmed_count,
        "command_count_delta": command_count_delta,
        "order_command_count_delta": order_command_count_delta,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
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
            "# Order Broker-Boundary Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- boundary_count: `{verdict.get('boundary_count')}`",
            f"- durable_pre_ack_count: `{verdict.get('durable_pre_ack_count')}`",
            f"- unconfirmed_count: `{verdict.get('unconfirmed_count')}`",
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            (
                "- order_command_count_delta: "
                f"`{verdict.get('order_command_count_delta')}`"
            ),
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "This check is read-only and never polls, queues, or sends an order.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return (
        "Order broker-boundary: "
        f"{verdict.get('status')} boundaries={verdict.get('boundary_count')} "
        f"pre_ack={verdict.get('durable_pre_ack_count')} "
        f"unconfirmed={verdict.get('unconfirmed_count')}"
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
