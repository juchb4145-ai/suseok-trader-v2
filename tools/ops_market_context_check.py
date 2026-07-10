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
        description="Check PR-17 common market context snapshot safety and coherency."
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
    parser.add_argument("--run-rebuild", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_context"),
    )
    args = parser.parse_args()
    report = run_market_context_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        run_rebuild=args.run_rebuild,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_market_context_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    run_rebuild: bool,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    command_before = fetch_json(
        f"{base_url}/api/gateway/commands/status",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    rebuild = (
        fetch_json(
            f"{base_url}/api/operator/market-context/rebuild?live_safe=true",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        )
        if run_rebuild
        else {"ok": True, "data": {"status": "NOT_RUN"}}
    )
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "market_indexes,market_context,market_regime,projection_outbox",
            "timeout_budget_ms": "5000",
        }
    )
    report = {
        "generated_at": _now(),
        "core_url": base_url,
        "run_rebuild": bool(run_rebuild),
        "core_status": fetch_json(
            f"{base_url}/api/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "command_status_before": command_before,
        "rebuild": rebuild,
        "market_context_status": fetch_json(
            f"{base_url}/api/operator/market-context/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "market_index_status": fetch_json(
            f"{base_url}/api/market-indexes/status",
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
        "rebuild",
        "market_context_status",
        "market_index_status",
        "projection_outbox",
        "dashboard_snapshot",
        "command_status_after",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status_before")
    after = _data(report, "command_status_after")
    rebuild = _data(report, "rebuild")
    context = _data(report, "market_context_status")
    outbox = _data(report, "projection_outbox")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_context = dashboard.get("market_context")
    dashboard_context = (
        dict(dashboard_context) if isinstance(dashboard_context, Mapping) else {}
    )

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

    latest = context.get("latest")
    latest = dict(latest) if isinstance(latest, Mapping) else {}
    if int(context.get("snapshot_count") or 0) < 2:
        warnings.append("MARKET_CONTEXT_SNAPSHOT_PAIR_MISSING")
    if not all(isinstance(latest.get(market), Mapping) for market in ("KOSPI", "KOSDAQ")):
        warnings.append("MARKET_CONTEXT_LATEST_PAIR_MISSING")
    if not bool(context.get("latest_watermark_coherent")):
        failures.append("MARKET_CONTEXT_WATERMARK_INCOHERENT")
    if not bool(context.get("latest_regime_coherent")):
        failures.append("MARKET_CONTEXT_REGIME_INCOHERENT")
    if int(context.get("regime_reference_missing_count") or 0):
        failures.append("MARKET_CONTEXT_REGIME_REFERENCE_MISSING")
    if context.get("stale_markets"):
        warnings.append("MARKET_CONTEXT_STALE")
    if context.get("parser_unverified_markets"):
        warnings.append("MARKET_CONTEXT_PARSER_UNVERIFIED")
    if context.get("data_unusable_markets"):
        warnings.append("MARKET_CONTEXT_DATA_UNUSABLE")
    if int(context.get("candidate_missing_snapshot_count") or 0):
        failures.append("CANDIDATE_MARKET_CONTEXT_REFERENCE_MISSING")
    if int(context.get("candidate_unreferenced_count") or 0):
        warnings.append("CANDIDATE_MARKET_CONTEXT_UNREFERENCED")
    if str(context.get("status") or "") != "PASS":
        warnings.append("MARKET_CONTEXT_STATUS_NOT_PASS")

    if report.get("run_rebuild"):
        if str(rebuild.get("status") or "") not in {"APPLIED", "APPLIED_BY_VERIFY"}:
            failures.append("MARKET_CONTEXT_REBUILD_NOT_APPLIED")
        if not bool(rebuild.get("observe_safe")):
            failures.append("MARKET_CONTEXT_REBUILD_NOT_OBSERVE_SAFE")
        if not bool(rebuild.get("no_trading_side_effects")):
            failures.append("MARKET_CONTEXT_REBUILD_SIDE_EFFECT_GUARD_MISSING")

    if not dashboard_context:
        failures.append("DASHBOARD_MARKET_CONTEXT_MISSING")
    elif dashboard_context.get("latest_watermark_coherent") != context.get(
        "latest_watermark_coherent"
    ):
        failures.append("DASHBOARD_MARKET_CONTEXT_MISMATCH")

    outbox_error_count, outbox_dead_letter_count = _outbox_error_counts(outbox)
    if outbox_error_count:
        failures.append("PROJECTION_OUTBOX_ERROR_PRESENT")
    if outbox_dead_letter_count:
        failures.append("PROJECTION_OUTBOX_DEAD_LETTER_PRESENT")

    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "snapshot_count": int(context.get("snapshot_count") or 0),
        "latest_watermark_coherent": bool(context.get("latest_watermark_coherent")),
        "latest_regime_coherent": bool(context.get("latest_regime_coherent")),
        "regime_reference_missing_count": int(
            context.get("regime_reference_missing_count") or 0
        ),
        "parser_unverified_markets": list(
            context.get("parser_unverified_markets") or []
        ),
        "data_unusable_markets": list(context.get("data_unusable_markets") or []),
        "candidate_reference_count": int(
            context.get("candidate_reference_count") or 0
        ),
        "candidate_unreferenced_count": int(
            context.get("candidate_unreferenced_count") or 0
        ),
        "candidate_missing_snapshot_count": int(
            context.get("candidate_missing_snapshot_count") or 0
        ),
        "outbox_error_count": outbox_error_count,
        "outbox_dead_letter_count": outbox_dead_letter_count,
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
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
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return "\n".join(
        [
            "# Common Market Context Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- snapshot_count: `{verdict.get('snapshot_count')}`",
            (
                "- latest_watermark_coherent: "
                f"`{verdict.get('latest_watermark_coherent')}`"
            ),
            f"- latest_regime_coherent: `{verdict.get('latest_regime_coherent')}`",
            (
                "- regime_reference_missing_count: "
                f"`{verdict.get('regime_reference_missing_count')}`"
            ),
            (
                "- parser_unverified_markets: `"
                f"{','.join(verdict.get('parser_unverified_markets') or []) or '-'}`"
            ),
            (
                "- data_unusable_markets: `"
                f"{','.join(verdict.get('data_unusable_markets') or []) or '-'}`"
            ),
            (
                "- candidate_reference_count/missing: "
                f"`{verdict.get('candidate_reference_count')}/"
                f"{verdict.get('candidate_missing_snapshot_count')}`"
            ),
            (
                "- candidate_unreferenced_count: "
                f"`{verdict.get('candidate_unreferenced_count')}`"
            ),
            (
                "- outbox ERROR/DEAD_LETTER: "
                f"`{verdict.get('outbox_error_count')}/"
                f"{verdict.get('outbox_dead_letter_count')}`"
            ),
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            (
                "- order_command_count_delta: "
                f"`{verdict.get('order_command_count_delta')}`"
            ),
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "The optional rebuild is OBSERVE-only and never submits trading commands.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return (
        "Market context: "
        f"{verdict.get('status')} snapshots={verdict.get('snapshot_count')} "
        f"coherent={verdict.get('latest_watermark_coherent')} "
        f"outbox={verdict.get('outbox_error_count')}/"
        f"{verdict.get('outbox_dead_letter_count')}"
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


def _outbox_error_counts(status: Mapping[str, Any]) -> tuple[int, int]:
    by_projection = status.get("by_projection_name")
    if isinstance(by_projection, Mapping):
        rows = [value for value in by_projection.values() if isinstance(value, Mapping)]
        return (
            sum(int(row.get("error_count") or 0) for row in rows),
            sum(int(row.get("dead_letter_count") or 0) for row in rows),
        )
    return (
        int(status.get("error_count") or 0),
        int(status.get("dead_letter_count") or 0),
    )


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
