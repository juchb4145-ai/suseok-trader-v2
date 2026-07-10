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
        description="Check PR-16 market_index limited-cutover readiness."
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
        default=str(ROOT_DIR / "reports" / "market_index_projection"),
    )
    args = parser.parse_args()
    report = run_market_index_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        timeout_sec=args.timeout_sec,
        run_worker=args.run_worker,
        expect_effective_skip=args.expect_effective_skip,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_market_index_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    timeout_sec: float,
    run_worker: bool,
    expect_effective_skip: bool = False,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": (
                "gateway,market_indexes,projection_outbox,"
                "market_index_projection_reconcile,"
                "market_index_append_only_routing,errors"
            ),
            "timeout_budget_ms": "5000",
        }
    )
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
                        else {"projection_name": "market_index"}
                    ),
                    "limit": 2 if expect_effective_skip else 1,
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
        f"{base_url}/api/operator/market-index-projection-reconcile/run-once?"
        + urllib.parse.urlencode(
            {"limit": limit, "persist": "true", "live_safe": "true"}
        ),
        token=token,
        method="POST",
        timeout_sec=timeout_sec,
    )
    report = {
        "generated_at": _now(),
        "core_url": base_url,
        "run_worker": bool(run_worker),
        "expect_effective_skip": bool(expect_effective_skip),
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
            f"{base_url}/api/operator/market-index-projection-reconcile/latest",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "routing_status": fetch_json(
            f"{base_url}/api/operator/market-index-append-only-routing/status",
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
    api_keys = (
        "core_status",
        "command_status_before",
        "worker_run",
        "reconcile_run",
        "latest_reconcile",
        "routing_status",
        "projection_outbox",
        "dashboard_snapshot",
        "command_status_after",
    )
    for key in api_keys:
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status_before")
    after = _data(report, "command_status_after")
    worker = _data(report, "worker_run")
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

    command_delta = _command_count(after) - _command_count(before)
    order_command_delta = int(after.get("order_command_count") or 0) - int(
        before.get("order_command_count") or 0
    )
    if command_delta:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")

    reconcile_status = str(reconcile.get("status") or "").upper()
    checked_event_count = int(reconcile.get("checked_event_count") or 0)
    data_unusable_count = int(reconcile.get("data_unusable_count") or 0)
    parser_unverified_count = int(reconcile.get("parser_unverified_count") or 0)
    tr_bootstrap_count = int(reconcile.get("tr_bootstrap_source_count") or 0)
    unknown_source_count = int(reconcile.get("unknown_source_count") or 0)
    if reconcile_status == "FAIL":
        failures.append("MARKET_INDEX_RECONCILE_FAIL")
    elif reconcile_status == "WARN":
        warnings.append("MARKET_INDEX_RECONCILE_WARN")
    if checked_event_count <= 0:
        warnings.append("MARKET_INDEX_EVENT_MISSING")
    if data_unusable_count:
        failures.append("MARKET_INDEX_DATA_NOT_USABLE")
    if tr_bootstrap_count:
        failures.append("MARKET_INDEX_TR_BOOTSTRAP_SOURCE_NOT_IMPLEMENTED")
    if parser_unverified_count:
        (failures if report.get("expect_effective_skip") else warnings).append(
            "MARKET_INDEX_PARSER_UNVERIFIED"
        )
    if unknown_source_count:
        (failures if report.get("expect_effective_skip") else warnings).append(
            "MARKET_INDEX_SOURCE_UNKNOWN"
        )

    by_projection = outbox.get("by_projection_name")
    index_outbox = (
        by_projection.get("market_index", {})
        if isinstance(by_projection, Mapping)
        else {}
    )
    outbox_error_count = int(index_outbox.get("error_count") or 0)
    outbox_dead_letter_count = int(index_outbox.get("dead_letter_count") or 0)
    if outbox_error_count or outbox_dead_letter_count:
        failures.append("MARKET_INDEX_OUTBOX_ERROR_OR_DEAD_LETTER")

    effective_skip_count = int(routing.get("effective_skip_inline_count") or 0)
    expect_effective_skip = bool(report.get("expect_effective_skip"))
    if expect_effective_skip and reconcile_status != "PASS":
        failures.append("MARKET_INDEX_RECONCILE_NOT_PASS")
    if expect_effective_skip and not report.get("run_worker"):
        failures.append("MARKET_INDEX_EXPECT_SKIP_REQUIRES_WORKER_RUN")
    if effective_skip_count and not expect_effective_skip:
        failures.append("MARKET_INDEX_EFFECTIVE_SKIP_FORBIDDEN_IN_PR15")
    if expect_effective_skip and effective_skip_count <= 0:
        failures.append("MARKET_INDEX_EFFECTIVE_SKIP_NOT_OBSERVED")
    if expect_effective_skip and not bool(routing.get("cutover_enabled")):
        failures.append("MARKET_INDEX_CUTOVER_DISABLED")
    if expect_effective_skip and bool(routing.get("global_kill_switch")):
        failures.append("MARKET_INDEX_GLOBAL_KILL_SWITCH")
    if expect_effective_skip and bool(
        routing.get("effective_skip_disabled_in_pr15")
    ):
        failures.append("MARKET_INDEX_LEGACY_SKIP_GUARD_ENABLED")
    if expect_effective_skip and int(routing.get("skip_budget_limit") or 0) != 1:
        failures.append("MARKET_INDEX_SKIP_BUDGET_NOT_ONE")
    if expect_effective_skip and str(routing.get("status") or "") != "PASS":
        failures.append("MARKET_INDEX_CONTROLLER_NOT_PASS")
    if expect_effective_skip and not bool(routing.get("observe_safe")):
        failures.append("MARKET_INDEX_ROUTING_NOT_OBSERVE_SAFE")
    if expect_effective_skip:
        required_true_fields = (
            "reconcile_required",
            "data_usable_required",
            "parser_verified_required",
            "regime_continuity_required",
            "regime_refresh_fail_closed",
            "gateway_health_required",
        )
        for field_name in required_true_fields:
            if not bool(routing.get(field_name)):
                failures.append(f"MARKET_INDEX_{field_name.upper()}_DISABLED")
        if not bool(routing.get("gateway_health_ready")):
            failures.append("MARKET_INDEX_GATEWAY_HEALTH_NOT_READY")
    if expect_effective_skip and bool(routing.get("rollback_required")):
        failures.append("MARKET_INDEX_ROLLBACK_REQUIRED")
    effective_health = _mapping(routing.get("effective_skip_health"))
    if expect_effective_skip:
        latest_decision = _mapping(routing.get("latest_decision"))
        latest_evidence = _mapping(latest_decision.get("evidence"))
        latest_gateway_health = _mapping(latest_evidence.get("gateway_health"))
        if not bool(latest_decision.get("effective_skip_inline")):
            failures.append("MARKET_INDEX_LATEST_DECISION_NOT_EFFECTIVE_SKIP")
        if not bool(latest_gateway_health.get("ready")):
            failures.append("MARKET_INDEX_LATEST_DECISION_GATEWAY_HEALTH_NOT_READY")
        if int(latest_evidence.get("skip_budget_limit") or 0) != 1:
            failures.append("MARKET_INDEX_LATEST_DECISION_BUDGET_NOT_ONE")
        if str(latest_evidence.get("event_market_session") or "") != "REGULAR":
            failures.append("MARKET_INDEX_LATEST_DECISION_NOT_KRX_REGULAR")
        if not bool(latest_evidence.get("event_market_weekday")):
            failures.append("MARKET_INDEX_LATEST_DECISION_NON_TRADING_DAY")
        event_age_sec = float(latest_evidence.get("event_age_sec") or 0.0)
        max_event_age_sec = float(latest_evidence.get("max_event_age_sec") or 0.0)
        if max_event_age_sec <= 0 or event_age_sec > max_event_age_sec:
            failures.append("MARKET_INDEX_LATEST_DECISION_EVENT_STALE")
        future_skew_sec = float(
            latest_evidence.get("event_future_skew_sec") or 0.0
        )
        max_future_skew_sec = float(
            latest_evidence.get("max_future_skew_sec") or 0.0
        )
        if (
            "max_future_skew_sec" not in latest_evidence
            or max_future_skew_sec < 0
            or future_skew_sec > max_future_skew_sec
        ):
            failures.append("MARKET_INDEX_LATEST_DECISION_FUTURE_SKEW")
        for key in (
            "pending_worker_count",
            "worker_error_count",
            "worker_apply_evidence_missing_count",
            "artifact_missing_count",
            "regime_snapshot_missing_count",
            "regime_outbox_not_applied_count",
        ):
            if int(effective_health.get(key) or 0):
                failures.append(f"MARKET_INDEX_{key.upper()}")
        if not bool(routing.get("regime_continuity_ready")):
            failures.append("MARKET_INDEX_REGIME_CONTINUITY_NOT_READY")
    if str(routing.get("tr_bootstrap_adapter_status") or "") != "NOT_IMPLEMENTED":
        failures.append("MARKET_INDEX_TR_BOOTSTRAP_STATUS_NOT_EXPLICIT")
    if report.get("run_worker"):
        if str(worker.get("status") or "") not in {"COMPLETED", "NOOP"}:
            failures.append("MARKET_INDEX_WORKER_NOT_COMPLETED")
        if worker.get("projection_name_filter") != "market_index":
            if not expect_effective_skip or worker.get("projection_name_filter") is not None:
                failures.append("MARKET_INDEX_WORKER_FILTER_MISMATCH")
        mutated = {str(value) for value in worker.get("mutated_projection_names") or []}
        allowed_mutations = (
            {"market_index", "market_regime", "market_context"}
            if expect_effective_skip
            else {"market_index"}
        )
        if mutated - allowed_mutations:
            failures.append("MARKET_INDEX_WORKER_MUTATED_OTHER_PROJECTION")
        if expect_effective_skip and not {
            "market_index",
            "market_regime",
            "market_context",
        } <= mutated:
            failures.append("MARKET_INDEX_REGIME_WORKER_MUTATION_MISSING")
        if not bool(worker.get("no_trading_side_effects")):
            failures.append("MARKET_INDEX_WORKER_TRADING_SIDE_EFFECT_GUARD_MISSING")

    if expect_effective_skip:
        regime_outbox = (
            by_projection.get("market_regime", {})
            if isinstance(by_projection, Mapping)
            else {}
        )
        if int(regime_outbox.get("error_count") or 0) or int(
            regime_outbox.get("dead_letter_count") or 0
        ):
            failures.append("MARKET_REGIME_OUTBOX_ERROR_OR_DEAD_LETTER")

    if not isinstance(dashboard.get("market_indexes"), Mapping):
        failures.append("DASHBOARD_MARKET_INDEX_SECTION_MISSING")
    if not isinstance(dashboard.get("market_index_projection_reconcile"), Mapping):
        failures.append("DASHBOARD_MARKET_INDEX_RECONCILE_MISSING")
    if not isinstance(dashboard.get("market_index_append_only_routing"), Mapping):
        failures.append("DASHBOARD_MARKET_INDEX_ROUTING_MISSING")

    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_pr16": bool(failures),
        "reconcile_status": reconcile_status or None,
        "checked_event_count": checked_event_count,
        "data_unusable_count": data_unusable_count,
        "parser_unverified_count": parser_unverified_count,
        "tr_bootstrap_source_count": tr_bootstrap_count,
        "unknown_source_count": unknown_source_count,
        "outbox_error_count": outbox_error_count,
        "outbox_dead_letter_count": outbox_dead_letter_count,
        "effective_skip_inline_count": effective_skip_count,
        "expect_effective_skip": expect_effective_skip,
        "controller_status": routing.get("status"),
        "rollback_required": bool(routing.get("rollback_required")),
        "skip_budget_limit": int(routing.get("skip_budget_limit") or 0),
        "regime_continuity_ready": bool(routing.get("regime_continuity_ready")),
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
    }


def write_report(report: dict[str, Any], *, out_dir: Path) -> dict[str, Path]:
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


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        f"market_index projection: {verdict.get('status')} "
        f"reconcile={verdict.get('reconcile_status')} "
        f"events={verdict.get('checked_event_count')} "
        f"effective_skip={verdict.get('effective_skip_inline_count')}"
    )


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return "\n".join(
        [
            "# Market Index Projection Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- reconcile_status: `{verdict.get('reconcile_status')}`",
            f"- checked_event_count: `{verdict.get('checked_event_count')}`",
            f"- data_unusable_count: `{verdict.get('data_unusable_count')}`",
            f"- parser_unverified_count: `{verdict.get('parser_unverified_count')}`",
            f"- tr_bootstrap_source_count: `{verdict.get('tr_bootstrap_source_count')}`",
            f"- unknown_source_count: `{verdict.get('unknown_source_count')}`",
            f"- outbox_error_count: `{verdict.get('outbox_error_count')}`",
            f"- outbox_dead_letter_count: `{verdict.get('outbox_dead_letter_count')}`",
            f"- effective_skip_inline_count: `{verdict.get('effective_skip_inline_count')}`",
            f"- expect_effective_skip: `{verdict.get('expect_effective_skip')}`",
            f"- controller_status: `{verdict.get('controller_status')}`",
            f"- rollback_required: `{verdict.get('rollback_required')}`",
            f"- skip_budget_limit: `{verdict.get('skip_budget_limit')}`",
            f"- regime_continuity_ready: `{verdict.get('regime_continuity_ready')}`",
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "PR-16 permits only guarded, budgeted market_index effective skips.",
            "Worker evidence must close both market_index and event-linked market_regime.",
            "TR bootstrap is not implemented; NXT is not KRX market-index evidence.",
        ]
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
    counts = payload.get("status_counts")
    if isinstance(counts, Mapping):
        return sum(int(value or 0) for value in counts.values())
    return int(payload.get("total_count") or 0)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
