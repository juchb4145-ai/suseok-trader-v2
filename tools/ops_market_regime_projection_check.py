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
        description="Check PR-18 market_regime worker, reconcile, and dry-run routing."
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
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_regime_projection"),
    )
    args = parser.parse_args()
    report = run_market_regime_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        worker_limit=args.worker_limit,
        timeout_sec=args.timeout_sec,
        run_worker=args.run_worker,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_market_regime_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    worker_limit: int,
    timeout_sec: float,
    run_worker: bool,
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
                    "projection_name": "market_regime",
                    "limit": min(max(int(worker_limit), 1), 20),
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
        f"{base_url}/api/operator/market-regime-projection-reconcile/run-once?"
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
                "market_context,projection_outbox,"
                "market_regime_projection_reconcile,"
                "market_regime_append_only_routing,errors"
            ),
            "timeout_budget_ms": "5000",
        }
    )
    report = {
        "generated_at": _now(),
        "core_url": base_url,
        "run_worker": bool(run_worker),
        "core_status": fetch_json(
            f"{base_url}/api/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "command_status_before": command_before,
        "worker_run": worker_run,
        "reconcile_run": reconcile_run,
        "latest_reconcile": fetch_json(
            f"{base_url}/api/operator/market-regime-projection-reconcile/latest",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "routing_status": fetch_json(
            f"{base_url}/api/operator/market-regime-append-only-routing/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "market_context_status": fetch_json(
            f"{base_url}/api/operator/market-context/status",
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
        "market_context_status",
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
    worker = _data(report, "worker_run")
    reconcile = _data(report, "reconcile_run")
    latest = _data(report, "latest_reconcile")
    routing = _data(report, "routing_status")
    context = _data(report, "market_context_status")
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
        failures.append("MARKET_REGIME_RECONCILE_FAIL")
    elif reconcile_status != "PASS":
        warnings.append("MARKET_REGIME_RECONCILE_NOT_PASS")
    if not bool(reconcile.get("append_only_ready")):
        warnings.append("MARKET_REGIME_APPEND_ONLY_NOT_READY")
    latest_run = latest.get("latest_run")
    latest_run = dict(latest_run) if isinstance(latest_run, Mapping) else {}
    if latest_run and latest_run.get("run_id") != reconcile.get("run_id"):
        failures.append("MARKET_REGIME_LATEST_RECONCILE_MISMATCH")

    if str(context.get("status") or "") != "PASS":
        warnings.append("MARKET_CONTEXT_NOT_PASS")
    if not bool(context.get("latest_watermark_coherent")):
        failures.append("MARKET_CONTEXT_WATERMARK_INCOHERENT")
    if not bool(context.get("latest_regime_coherent")):
        failures.append("MARKET_CONTEXT_REGIME_INCOHERENT")

    regime_outbox = _projection_status(outbox, "market_regime")
    outbox_error_count = int(regime_outbox.get("error_count") or 0)
    outbox_dead_letter_count = int(regime_outbox.get("dead_letter_count") or 0)
    if outbox_error_count:
        failures.append("MARKET_REGIME_OUTBOX_ERROR_PRESENT")
    if outbox_dead_letter_count:
        failures.append("MARKET_REGIME_OUTBOX_DEAD_LETTER_PRESENT")

    effective_skip_count = int(routing.get("effective_skip_inline_count") or 0)
    if effective_skip_count:
        failures.append("MARKET_REGIME_EFFECTIVE_SKIP_FORBIDDEN_IN_PR18")
    if not bool(routing.get("effective_skip_disabled_in_pr18")):
        failures.append("MARKET_REGIME_PR18_SKIP_GUARD_DISABLED")

    if report.get("run_worker"):
        if str(worker.get("status") or "") not in {"COMPLETED", "NOOP"}:
            failures.append("MARKET_REGIME_WORKER_NOT_COMPLETED")
        if worker.get("projection_name_filter") != "market_regime":
            failures.append("MARKET_REGIME_WORKER_FILTER_MISMATCH")
        if not bool(worker.get("market_regime_apply_enabled")):
            failures.append("MARKET_REGIME_WORKER_APPLY_DISABLED")
        mutations = {
            str(value) for value in worker.get("mutated_projection_names") or []
        }
        if mutations - {"market_regime", "market_context"}:
            failures.append("MARKET_REGIME_WORKER_MUTATED_OTHER_PROJECTION")
        if not bool(worker.get("no_trading_side_effects")):
            failures.append("MARKET_REGIME_WORKER_SIDE_EFFECT_GUARD_MISSING")

    for section in (
        "market_regime_projection_reconcile",
        "market_regime_append_only_routing",
    ):
        if not isinstance(dashboard.get(section), Mapping):
            failures.append(f"DASHBOARD_{section.upper()}_MISSING")

    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_pr18": bool(failures),
        "reconcile_status": reconcile_status or None,
        "append_only_ready": bool(reconcile.get("append_only_ready")),
        "checked_event_count": int(reconcile.get("checked_event_count") or 0),
        "context_status": context.get("status"),
        "outbox_error_count": outbox_error_count,
        "outbox_dead_letter_count": outbox_dead_letter_count,
        "would_skip_inline_count": int(routing.get("would_skip_inline_count") or 0),
        "effective_skip_inline_count": effective_skip_count,
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
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
            "# Market Regime Projection PR-18 Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- reconcile: `{verdict.get('reconcile_status')}`",
            f"- append_only_ready: `{verdict.get('append_only_ready')}`",
            f"- checked_event_count: `{verdict.get('checked_event_count')}`",
            f"- context_status: `{verdict.get('context_status')}`",
            (
                "- outbox ERROR/DEAD_LETTER: "
                f"`{verdict.get('outbox_error_count')}/"
                f"{verdict.get('outbox_dead_letter_count')}`"
            ),
            (
                "- would/effective skip: "
                f"`{verdict.get('would_skip_inline_count')}/"
                f"{verdict.get('effective_skip_inline_count')}`"
            ),
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "PR-18 records dry-run evidence only; effective inline skip must remain zero.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "Market regime PR-18: "
        f"{verdict.get('status')} reconcile={verdict.get('reconcile_status')} "
        f"events={verdict.get('checked_event_count')} "
        f"effective_skip={verdict.get('effective_skip_inline_count')}"
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    data = payload.get("data")
    return dict(data) if isinstance(data, Mapping) else dict(payload)


def _projection_status(
    status: Mapping[str, Any], projection_name: str
) -> dict[str, Any]:
    by_projection = status.get("by_projection_name")
    if not isinstance(by_projection, Mapping):
        return {}
    value = by_projection.get(projection_name)
    return dict(value) if isinstance(value, Mapping) else {}


def _command_count(status: Mapping[str, Any]) -> int:
    counts = status.get("counts")
    if not isinstance(counts, Mapping):
        return 0
    return sum(int(value or 0) for value in counts.values())


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
