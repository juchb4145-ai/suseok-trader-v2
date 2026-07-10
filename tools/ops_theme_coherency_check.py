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

PROVENANCE_FIELDS = {
    "source",
    "snapshot_id",
    "calculated_at",
    "data_age_sec",
    "watchset_selection_source",
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check DB top-theme and leadership source coherency."
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
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "theme_coherency"),
    )
    args = parser.parse_args()
    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        limit=min(max(args.limit, 1), 100),
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    limit: int,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "theme_coherency,pipeline_summary",
            "timeout_budget_ms": "5000",
        }
    )
    report: dict[str, Any] = {
        "generated_at": _now(),
        "core_url": base_url,
        "read_only": True,
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
        "theme_coherency": fetch_json(
            f"{base_url}/api/operator/theme-coherency/status?limit={limit}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "operator_status": fetch_json(
            f"{base_url}/api/operator/status",
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
        "theme_coherency",
        "operator_status",
        "dashboard_snapshot",
        "command_status_after",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status_before")
    after = _data(report, "command_status_after")
    coherency = _data(report, "theme_coherency")
    operator = _data(report, "operator_status")
    dashboard = _data(report, "dashboard_snapshot")
    operator_coherency = _mapping(operator.get("theme_coherency"))
    dashboard_coherency = _mapping(dashboard.get("theme_coherency"))
    summary_coherency = _mapping(
        _mapping(dashboard.get("pipeline_summary")).get("theme_coherency")
    )

    if core.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core.get("live_sim_allowed")) or bool(core.get("live_real_allowed")):
        failures.append("LIVE_TRADING_ALLOWED")
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

    status = str(coherency.get("status") or "")
    if status == "FAIL":
        failures.append("THEME_COHERENCY_FAIL")
    elif status == "WARN":
        warnings.extend(str(code) for code in coherency.get("reason_codes") or [])
    elif status != "PASS":
        failures.append("THEME_COHERENCY_STATUS_UNKNOWN")
    if int(coherency.get("snapshot_mismatch_count") or 0):
        failures.append("THEME_SNAPSHOT_MISMATCH_PRESENT")
    if int(coherency.get("missing_snapshot_count") or 0):
        failures.append("THEME_SNAPSHOT_MISSING")

    for section_name, section in (
        ("DB_TOP", _mapping(coherency.get("db_top"))),
        ("LEADERSHIP", _mapping(coherency.get("leadership"))),
    ):
        if not PROVENANCE_FIELDS <= section.keys():
            failures.append(f"{section_name}_PROVENANCE_FIELDS_MISSING")
    for item in [
        *list(coherency.get("db_top_items") or []),
        *list(coherency.get("leadership_items") or []),
    ]:
        if not isinstance(item, Mapping) or not PROVENANCE_FIELDS <= item.keys():
            failures.append("THEME_ITEM_PROVENANCE_FIELDS_MISSING")
            break

    expected_status = coherency.get("status")
    for name, comparable in (
        ("OPERATOR", operator_coherency),
        ("DASHBOARD", dashboard_coherency),
        ("DASHBOARD_SUMMARY", summary_coherency),
    ):
        if not comparable:
            failures.append(f"{name}_THEME_COHERENCY_MISSING")
        elif comparable.get("status") != expected_status:
            failures.append(f"{name}_THEME_COHERENCY_MISMATCH")

    verdict_status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict_status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "db_top_count": int(coherency.get("db_top_count") or 0),
        "leadership_top_count": int(coherency.get("leadership_top_count") or 0),
        "source_mismatch_count": int(coherency.get("source_mismatch_count") or 0),
        "snapshot_mismatch_count": int(
            coherency.get("snapshot_mismatch_count") or 0
        ),
        "top_set_mismatch_count": int(
            coherency.get("top_set_mismatch_count") or 0
        ),
        "stale_count": int(coherency.get("stale_count") or 0),
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "read_only": True,
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
            "# Theme Coherency Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            (
                "- DB top / leadership: "
                f"`{verdict.get('db_top_count')}/"
                f"{verdict.get('leadership_top_count')}`"
            ),
            f"- source mismatch: `{verdict.get('source_mismatch_count')}`",
            f"- snapshot mismatch: `{verdict.get('snapshot_mismatch_count')}`",
            f"- top-set mismatch: `{verdict.get('top_set_mismatch_count')}`",
            f"- stale: `{verdict.get('stale_count')}`",
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "This check is read-only and never creates an order command.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "Theme coherency: "
        f"{verdict.get('status')} db={verdict.get('db_top_count')} "
        f"leadership={verdict.get('leadership_top_count')} "
        f"snapshot_mismatch={verdict.get('snapshot_mismatch_count')}"
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
    counts = status.get("counts")
    if not isinstance(counts, Mapping):
        return 0
    return sum(int(value or 0) for value in counts.values())


if __name__ == "__main__":
    raise SystemExit(main())
