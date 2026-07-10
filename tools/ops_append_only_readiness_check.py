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
        description="Check final append-only readiness without changing runtime flags."
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
    parser.add_argument("--expect-ready", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "append_only_readiness"),
    )
    args = parser.parse_args()
    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        expect_ready=args.expect_ready,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    expect_ready: bool,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "append_only_readiness",
            "timeout_budget_ms": "5000",
        }
    )
    report: dict[str, Any] = {
        "generated_at": _now(),
        "core_url": base_url,
        "expect_ready": expect_ready,
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
        "readiness": fetch_json(
            f"{base_url}/api/operator/append-only-readiness/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "dashboard": fetch_json(
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
        "readiness",
        "dashboard",
        "command_status_after",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status_before")
    readiness = _data(report, "readiness")
    dashboard = _mapping(_data(report, "dashboard").get("append_only_readiness"))
    after = _data(report, "command_status_after")
    if core.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core.get("live_sim_allowed")) or bool(core.get("live_real_allowed")):
        failures.append("LIVE_TRADING_ALLOWED")

    readiness_status = str(readiness.get("status") or "")
    allowed_statuses = {
        "BLOCKED_SCHEMA",
        "BLOCKED_CONFIG",
        "BLOCKED_HEALTH",
        "BLOCKED_EVIDENCE",
        "READY_FOR_OPERATOR_REVIEW",
    }
    if readiness_status not in allowed_statuses:
        failures.append("APPEND_ONLY_READINESS_STATUS_INVALID")
    if report.get("expect_ready"):
        if readiness_status != "READY_FOR_OPERATOR_REVIEW":
            failures.append("APPEND_ONLY_READINESS_NOT_READY")
    elif readiness_status != "READY_FOR_OPERATOR_REVIEW":
        warnings.append("APPEND_ONLY_READINESS_BLOCKED_SAFE")

    if readiness.get("read_only") is not True:
        failures.append("READINESS_NOT_READ_ONLY")
    if readiness.get("automatic_cutover_allowed") is not False:
        failures.append("AUTOMATIC_CUTOVER_ALLOWED")
    if readiness.get("flag_cleanup_allowed") is not False:
        failures.append("FLAG_CLEANUP_ALLOWED_WITHOUT_OPERATOR_REVIEW")
    if readiness.get("request_path_removal_performed") is not False:
        failures.append("REQUEST_PATH_REMOVAL_REPORTED")
    if readiness.get("emergency_inline_fallback_retained") is not True:
        failures.append("EMERGENCY_INLINE_FALLBACK_NOT_RETAINED")
    if dashboard.get("status") != readiness_status:
        failures.append("DASHBOARD_READINESS_STATUS_MISMATCH")

    command_delta = _command_count(after) - _command_count(before)
    order_command_delta = int(after.get("order_command_count") or 0) - int(
        before.get("order_command_count") or 0
    )
    if command_delta:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")
    verdict_status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict_status,
        "readiness_status": readiness_status,
        "consecutive_qualified_trading_day_count": int(
            readiness.get("consecutive_qualified_trading_day_count") or 0
        ),
        "required_trading_days": int(readiness.get("required_trading_days") or 10),
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "block_flag_cleanup": readiness_status != "READY_FOR_OPERATOR_REVIEW",
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
            "# Append-only Readiness Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- readiness_status: `{verdict.get('readiness_status')}`",
            (
                "- consecutive evidence: "
                f"`{verdict.get('consecutive_qualified_trading_day_count')}/"
                f"{verdict.get('required_trading_days')}`"
            ),
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "This check is read-only and never changes cutover flags or inline fallback.",
            "Ten evidence days only permit operator review; they do not trigger cutover.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "Append-only readiness: "
        f"{verdict.get('status')} status={verdict.get('readiness_status')} "
        f"days={verdict.get('consecutive_qualified_trading_day_count')}/"
        f"{verdict.get('required_trading_days')}"
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    data = payload.get("data", payload)
    return dict(data) if isinstance(data, Mapping) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _command_count(payload: Mapping[str, Any]) -> int:
    for key in ("total_count", "command_count", "total"):
        if payload.get(key) is not None:
            return int(payload.get(key) or 0)
    counts = payload.get("counts")
    if isinstance(counts, Mapping):
        return sum(int(value or 0) for value in counts.values())
    return 0


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
