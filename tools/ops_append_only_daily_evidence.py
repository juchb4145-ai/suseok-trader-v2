from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import urllib.parse
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.runtime.append_only_readiness import REQUIRED_COMPONENTS  # noqa: E402
from tools.ops_market_data_tr_response_side_effect_check import (  # noqa: E402
    fetch_json,
)

KST = timezone(timedelta(hours=9), name="Asia/Seoul")
_READY_STATUSES = {"BLOCKED_EVIDENCE", "READY_FOR_OPERATOR_REVIEW"}
_RECONCILE_ENDPOINTS = {
    "market_data": "/api/operator/market-data-projection-reconcile/run-once",
    "market_reference": "/api/operator/market-reference-projection-reconcile/run-once",
    "market_index": "/api/operator/market-index-projection-reconcile/run-once",
    "market_regime": "/api/operator/market-regime-projection-reconcile/run-once",
    "market_scan": "/api/operator/market-scan-projection-reconcile/run-once",
}
_RECONCILE_MIN_TIMEOUT_SEC = {"market_scan": 120.0}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Close one persistent OBSERVE append-only evidence day after ingest stops."
        )
    )
    parser.add_argument(
        "--core-url",
        default=os.environ.get("TRADING_CORE_URL", "http://127.0.0.1:8040"),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("TRADING_CORE_TOKEN")
        or os.environ.get("GATEWAY_CORE_TOKEN", ""),
    )
    parser.add_argument("--expected-db-path", required=True)
    parser.add_argument("--session-state-path", required=True)
    parser.add_argument("--trade-date", default=_today_kst())
    parser.add_argument("--settle-sec", type=float, default=2.0)
    parser.add_argument("--drain-limit", type=int, default=500)
    parser.add_argument("--drain-max-batches", type=int, default=100)
    parser.add_argument("--reconcile-limit", type=int, default=5000)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "append_only_daily_evidence"),
    )
    args = parser.parse_args()

    try:
        expected_db_path = validate_persistent_db_path(args.expected_db_path)
        session_state_path, session_state = load_session_state(args.session_state_path)
        validate_current_trade_date(args.trade_date)
    except ValueError as exc:
        print(f"append-only daily evidence: FAIL {exc}")
        return 2
    if not args.token.strip():
        print("append-only daily evidence: FAIL local Core token is required")
        return 2

    report = run_daily_evidence_report(
        core_url=args.core_url,
        token=args.token,
        expected_db_path=expected_db_path,
        session_state_path=session_state_path,
        session_state=session_state,
        trade_date=args.trade_date,
        settle_sec=max(float(args.settle_sec), 0.0),
        drain_limit=min(max(int(args.drain_limit), 1), 500),
        drain_max_batches=min(max(int(args.drain_max_batches), 1), 500),
        reconcile_limit=min(max(int(args.reconcile_limit), 1), 5000),
        timeout_sec=max(float(args.timeout_sec), 1.0),
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] == "PASS" else 2


def run_daily_evidence_report(
    *,
    core_url: str,
    token: str,
    expected_db_path: str,
    session_state_path: str,
    session_state: Mapping[str, Any],
    trade_date: str,
    settle_sec: float,
    drain_limit: int,
    drain_max_batches: int,
    reconcile_limit: int,
    timeout_sec: float,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    report: dict[str, Any] = {
        "format": "append-only-daily-evidence/v1",
        "generated_at": _now(),
        "trade_date": trade_date,
        "core_url": base_url,
        "expected_db_path": expected_db_path,
        "session_state_path": session_state_path,
        "session_state": dict(session_state),
        "official_krx_calendar_status": "OPERATOR_CONFIRMED_AT_LAUNCH",
        "core_status": _fetch(base_url, "/api/status", token, timeout_sec),
        "initial_readiness": _fetch(
            base_url,
            "/api/operator/append-only-readiness/status",
            token,
            timeout_sec,
        ),
        "command_status_before": _fetch(
            base_url,
            "/api/gateway/commands/status",
            token,
            timeout_sec,
        ),
        "gateway_status_before_settle": _fetch(
            base_url,
            "/api/gateway/status",
            token,
            timeout_sec,
        ),
    }
    if settle_sec:
        time.sleep(settle_sec)
    report["gateway_status_after_settle"] = _fetch(
        base_url,
        "/api/gateway/status",
        token,
        timeout_sec,
    )
    preflight_failures = evaluate_preflight(report)
    report["preflight"] = {
        "status": "PASS" if not preflight_failures else "FAIL",
        "failures": preflight_failures,
        "mutating_actions_allowed": not preflight_failures,
    }

    if not preflight_failures:
        report["projection_outbox_drain"] = _drain_projection_outbox(
            base_url=base_url,
            token=token,
            limit=drain_limit,
            max_batches=drain_max_batches,
            timeout_sec=timeout_sec,
        )
        report["lifecycle_run"] = _fetch(
            base_url,
            "/api/operator/live-sim/lifecycle-consumer/run-once?limit=500",
            token,
            timeout_sec,
            method="POST",
        )
        report["market_context_rebuild"] = _fetch(
            base_url,
            "/api/operator/market-context/rebuild?live_safe=true",
            token,
            timeout_sec,
            method="POST",
        )
        report["reconcile_runs"] = {
            component: _fetch(
                base_url,
                endpoint
                + "?"
                + urllib.parse.urlencode(
                    {
                        "limit": reconcile_limit,
                        "persist": "true",
                        "live_safe": "true",
                    }
                ),
                token,
                max(
                    timeout_sec,
                    _RECONCILE_MIN_TIMEOUT_SEC.get(component, timeout_sec),
                ),
                method="POST",
            )
            for component, endpoint in _RECONCILE_ENDPOINTS.items()
        }
    else:
        report["projection_outbox_drain"] = {
            "status": "NOT_RUN",
            "reason_codes": ["PREFLIGHT_FAILED"],
        }
        report["lifecycle_run"] = {"status": "NOT_RUN"}
        report["market_context_rebuild"] = {"status": "NOT_RUN"}
        report["reconcile_runs"] = {}

    report["final_readiness"] = _fetch(
        base_url,
        "/api/operator/append-only-readiness/status",
        token,
        timeout_sec,
    )
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "append_only_readiness",
            "timeout_budget_ms": "5000",
        }
    )
    report["dashboard"] = _fetch(
        base_url,
        f"/api/dashboard/snapshot?{dashboard_query}",
        token,
        timeout_sec,
    )
    report["projection_outbox"] = _fetch(
        base_url,
        "/api/operator/projection-outbox/status",
        token,
        timeout_sec,
    )
    report["gateway_status_final"] = _fetch(
        base_url,
        "/api/gateway/status",
        token,
        timeout_sec,
    )
    report["command_status_after"] = _fetch(
        base_url,
        "/api/gateway/commands/status",
        token,
        timeout_sec,
    )
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_preflight(report: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    for key in (
        "core_status",
        "initial_readiness",
        "command_status_before",
        "gateway_status_before_settle",
        "gateway_status_after_settle",
    ):
        if not _api_ok(report.get(key)):
            failures.append(f"{key.upper()}_API_ERROR")
    core = _data(report, "core_status")
    readiness = _data(report, "initial_readiness")
    gateway_before = _data(report, "gateway_status_before_settle")
    gateway_after = _data(report, "gateway_status_after_settle")
    if str(core.get("profile") or "") != "OBSERVE" or str(
        core.get("mode") or ""
    ) != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core.get("live_sim_allowed")) or bool(core.get("live_real_allowed")):
        failures.append("LIVE_TRADING_ALLOWED")
    if _normalize_path(core.get("database_path")) != _normalize_path(
        report.get("expected_db_path")
    ):
        failures.append("CORE_DATABASE_PATH_MISMATCH")
    session_state = _mapping(report.get("session_state"))
    if session_state.get("trade_date") != report.get("trade_date"):
        failures.append("SESSION_STATE_TRADE_DATE_MISMATCH")
    if _normalize_path(session_state.get("database_path")) != _normalize_path(
        report.get("expected_db_path")
    ):
        failures.append("SESSION_STATE_DATABASE_PATH_MISMATCH")
    if str(session_state.get("core_url") or "").rstrip("/") != str(
        report.get("core_url") or ""
    ).rstrip("/"):
        failures.append("SESSION_STATE_CORE_URL_MISMATCH")
    configuration = _mapping(readiness.get("configuration"))
    if configuration.get("ready") is not True:
        failures.append("APPEND_ONLY_CONFIGURATION_NOT_ARMED")
    for field in ("last_heartbeat_at", "last_event_received_at"):
        if gateway_before.get(field) != gateway_after.get(field):
            failures.append("GATEWAY_INGEST_NOT_STOPPED")
            break
    return sorted(set(failures))


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures = list(_mapping(report.get("preflight")).get("failures") or [])
    for key in (
        "final_readiness",
        "dashboard",
        "projection_outbox",
        "gateway_status_final",
        "command_status_after",
    ):
        if not _api_ok(report.get(key)):
            failures.append(f"{key.upper()}_API_ERROR")
    for component, payload in _mapping(report.get("reconcile_runs")).items():
        if not _api_ok(payload):
            failures.append(f"{str(component).upper()}_RECONCILE_API_ERROR")

    if not _api_ok(report.get("market_context_rebuild")):
        failures.append("MARKET_CONTEXT_REBUILD_API_ERROR")
    market_context_rebuild = _data(report, "market_context_rebuild")
    if market_context_rebuild and str(
        market_context_rebuild.get("status") or ""
    ) not in {"APPLIED", "APPLIED_BY_VERIFY"}:
        failures.append("MARKET_CONTEXT_REBUILD_NOT_HEALTHY")

    drain = _mapping(report.get("projection_outbox_drain"))
    if drain.get("status") not in {"COMPLETED", "NOT_RUN"}:
        failures.append("PROJECTION_OUTBOX_DRAIN_INCOMPLETE")
    if int(drain.get("remaining_pending_count") or 0):
        failures.append("PROJECTION_OUTBOX_PENDING_REMAINS")
    if int(drain.get("error_count") or 0):
        failures.append("PROJECTION_OUTBOX_ERROR_PRESENT")
    if int(drain.get("dead_letter_count") or 0):
        failures.append("PROJECTION_OUTBOX_DEAD_LETTER_PRESENT")

    lifecycle = _data(report, "lifecycle_run")
    if lifecycle and str(lifecycle.get("status") or "") not in {"IDLE", "COMPLETED"}:
        failures.append("LIFECYCLE_RUN_NOT_HEALTHY")
    if int(lifecycle.get("error_count") or 0):
        failures.append("LIFECYCLE_RUN_ERROR_PRESENT")
    if int(lifecycle.get("dead_letter_count") or 0):
        failures.append("LIFECYCLE_RUN_DEAD_LETTER_PRESENT")

    readiness = _data(report, "final_readiness")
    readiness_status = str(readiness.get("status") or "")
    if readiness_status not in _READY_STATUSES:
        failures.append("FINAL_READINESS_NOT_DAILY_EVIDENCE_SAFE")
    if _mapping(readiness.get("current_health")).get("ready") is not True:
        failures.append("FINAL_CONSUMER_HEALTH_NOT_READY")
    component_results: dict[str, dict[str, Any]] = {}
    component_statuses = _mapping(readiness.get("component_statuses"))
    trade_date = str(report.get("trade_date") or "")
    for component in REQUIRED_COMPONENTS:
        status = _mapping(component_statuses.get(component))
        latest = _mapping(status.get("latest"))
        passed = bool(
            status.get("latest_trade_date") == trade_date
            and latest.get("run_date") == trade_date
            and latest.get("passed") is True
        )
        component_results[component] = {
            "passed": passed,
            "latest_trade_date": status.get("latest_trade_date"),
            "run_id": latest.get("run_id"),
            "status": latest.get("status"),
            "reason_codes": list(latest.get("reason_codes") or []),
        }
        if not passed:
            failures.append(f"{component.upper()}_DAILY_EVIDENCE_NOT_QUALIFIED")

    before_gateway = _data(report, "gateway_status_after_settle")
    final_gateway = _data(report, "gateway_status_final")
    for field in ("last_heartbeat_at", "last_event_received_at"):
        if before_gateway.get(field) != final_gateway.get(field):
            failures.append("INGEST_RESUMED_DURING_DAILY_CLOSE")
            break

    before_commands = _data(report, "command_status_before")
    after_commands = _data(report, "command_status_after")
    command_delta = _command_count(after_commands) - _command_count(before_commands)
    order_command_delta = int(after_commands.get("order_command_count") or 0) - int(
        before_commands.get("order_command_count") or 0
    )
    if command_delta:
        failures.append("COMMAND_COUNT_CHANGED_DURING_DAILY_CLOSE")
    if order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_DAILY_CLOSE")
    session_state = _mapping(report.get("session_state"))
    session_command_delta = _command_count(after_commands) - int(
        session_state.get("command_count") or 0
    )
    session_failed_command_delta = _failed_command_count(after_commands) - int(
        session_state.get("failed_command_count") or 0
    )
    session_order_command_delta = int(after_commands.get("order_command_count") or 0) - int(
        session_state.get("order_command_count") or 0
    )
    if session_failed_command_delta:
        failures.append("FAILED_COMMAND_COUNT_CHANGED_DURING_SESSION")
    if session_order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_SESSION")

    for field in (
        "automatic_cutover_allowed",
        "flag_cleanup_allowed",
        "raw_append_enqueue_only_enabled",
        "request_path_removal_performed",
    ):
        if readiness.get(field) is not False:
            failures.append(f"{field.upper()}_UNEXPECTEDLY_ENABLED")
    if readiness.get("emergency_inline_fallback_retained") is not True:
        failures.append("EMERGENCY_INLINE_FALLBACK_NOT_RETAINED")

    failures = sorted(set(failures))
    return {
        "status": "FAIL" if failures else "PASS",
        "readiness_status": readiness_status,
        "trade_date": trade_date,
        "component_results": component_results,
        "qualified_today": not any(
            failure.endswith("_DAILY_EVIDENCE_NOT_QUALIFIED")
            for failure in failures
        ),
        "consecutive_qualified_trading_day_count": int(
            readiness.get("consecutive_qualified_trading_day_count") or 0
        ),
        "required_trading_days": int(readiness.get("required_trading_days") or 10),
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "session_command_count_delta": session_command_delta,
        "session_failed_command_delta": session_failed_command_delta,
        "session_order_command_delta": session_order_command_delta,
        "failures": failures,
        "operator_warnings": ["OFFICIAL_KRX_CALENDAR_REVIEW_STILL_REQUIRED"],
        "automatic_cutover_allowed": False,
        "no_order_side_effects": command_delta == 0 and order_command_delta == 0,
        "no_trading_side_effects": True,
    }


def validate_persistent_db_path(value: str | os.PathLike[str]) -> str:
    path = os.path.abspath(os.fspath(value))
    if Path(path).suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
        raise ValueError("evidence DB must use .sqlite, .sqlite3, or .db")
    temp_root = os.path.abspath(tempfile.gettempdir())
    try:
        inside_temp = os.path.commonpath([path, temp_root]) == temp_root
    except ValueError:
        inside_temp = False
    if inside_temp:
        raise ValueError("persistent 10-day evidence DB cannot be stored under TEMP")
    if not Path(path).is_file():
        raise ValueError(f"evidence DB does not exist: {path}")
    return path


def load_session_state(
    value: str | os.PathLike[str],
) -> tuple[str, dict[str, Any]]:
    path = os.path.abspath(os.fspath(value))
    if not Path(path).is_file():
        raise ValueError(f"session state does not exist: {path}")
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"session state is invalid: {path}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"session state must be a JSON object: {path}")
    required = {
        "trade_date",
        "core_url",
        "database_path",
        "command_count",
        "failed_command_count",
        "order_command_count",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"session state fields missing: {','.join(missing)}")
    return path, dict(payload)


def validate_current_trade_date(value: str) -> None:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("trade date must use YYYY-MM-DD") from exc
    today = datetime.now(KST).date()
    if parsed != today:
        raise ValueError("historical or future evidence close is forbidden")
    if parsed.weekday() >= 5:
        raise ValueError("KRX evidence close is forbidden on weekends")


def _drain_projection_outbox(
    *,
    base_url: str,
    token: str,
    limit: int,
    max_batches: int,
    timeout_sec: float,
) -> dict[str, Any]:
    totals = {
        "batch_count": 0,
        "claimed_count": 0,
        "applied_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "dead_letter_count": 0,
        "remaining_pending_count": 0,
    }
    runs: list[dict[str, Any]] = []
    for _ in range(max_batches):
        payload = _fetch(
            base_url,
            "/api/operator/projection-outbox/run-once?"
            + urllib.parse.urlencode(
                {
                    "limit": limit,
                    "apply_projection": "true",
                    "live_safe": "true",
                }
            ),
            token,
            timeout_sec,
            method="POST",
        )
        run = _data_value(payload)
        runs.append(run)
        totals["batch_count"] += 1
        for field in (
            "claimed_count",
            "applied_count",
            "skipped_count",
            "error_count",
            "dead_letter_count",
        ):
            totals[field] += int(run.get(field) or 0)
        totals["remaining_pending_count"] = int(
            run.get("remaining_pending_count") or 0
        )
        if not _api_ok(payload) or str(run.get("status") or "").startswith("LOCKED"):
            break
        if totals["remaining_pending_count"] == 0:
            break
    totals["status"] = (
        "COMPLETED" if totals["remaining_pending_count"] == 0 else "INCOMPLETE"
    )
    totals["runs"] = runs[-10:]
    totals["no_trading_side_effects"] = True
    return totals


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    report_dir = out_dir / str(report.get("trade_date") or "unknown")
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    raw_path = report_dir / f"{stamp}_raw.json"
    summary_path = report_dir / f"{stamp}_summary.md"
    latest_path = report_dir / "latest.json"
    raw_text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    raw_path.write_text(raw_text, encoding="utf-8")
    latest_path.write_text(raw_text, encoding="utf-8")
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "latest_json": latest_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    components = _mapping(verdict.get("component_results"))
    lines = [
        "# Append-only Daily Evidence",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- trade_date: `{report.get('trade_date')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- readiness_status: `{verdict.get('readiness_status')}`",
        (
            "- consecutive evidence: "
            f"`{verdict.get('consecutive_qualified_trading_day_count')}/"
            f"{verdict.get('required_trading_days')}`"
        ),
        f"- command/order-command delta: `{verdict.get('command_count_delta')}/"
        f"{verdict.get('order_command_count_delta')}`",
        (
            "- session command/failed/order delta: "
            f"`{verdict.get('session_command_count_delta')}/"
            f"{verdict.get('session_failed_command_delta')}/"
            f"{verdict.get('session_order_command_delta')}`"
        ),
        f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
        "",
        "## Components",
        "",
        "| Component | Qualified | Run | Reasons |",
        "|---|---:|---|---|",
    ]
    for component in REQUIRED_COMPONENTS:
        item = _mapping(components.get(component))
        lines.append(
            f"| `{component}` | `{item.get('passed')}` | "
            f"`{item.get('run_id')}` | "
            f"`{', '.join(item.get('reason_codes') or []) or '-'}` |"
        )
    lines.extend(
        [
            "",
            "This tool never enables trading, changes cutover flags, or creates order commands.",
            "A qualified day still requires official KRX calendar review.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "append-only daily evidence: "
        f"{verdict.get('status')} date={verdict.get('trade_date')} "
        f"days={verdict.get('consecutive_qualified_trading_day_count')}/"
        f"{verdict.get('required_trading_days')}"
    )


def _fetch(
    base_url: str,
    path: str,
    token: str,
    timeout_sec: float,
    *,
    method: str = "GET",
) -> dict[str, Any]:
    return fetch_json(
        f"{base_url}{path}",
        token=token,
        method=method,
        timeout_sec=timeout_sec,
    )


def _api_ok(value: Any) -> bool:
    return isinstance(value, Mapping) and bool(value.get("ok", True))


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    return _data_value(report.get(key))


def _data_value(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    data = value.get("data", value)
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


def _failed_command_count(payload: Mapping[str, Any]) -> int:
    counts = payload.get("counts")
    if not isinstance(counts, Mapping):
        return 0
    return int(counts.get("FAILED") or 0)


def _normalize_path(value: Any) -> str:
    if value in (None, ""):
        return ""
    return os.path.normcase(os.path.abspath(os.fspath(value)))


def _today_kst() -> str:
    return datetime.now(KST).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
