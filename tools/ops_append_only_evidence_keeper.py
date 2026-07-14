from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tools.ops_market_data_tr_response_side_effect_check import (  # noqa: E402
    fetch_json,
    is_locked_retryable_payload,
)

KST = timezone(timedelta(hours=9), name="Asia/Seoul")
_RECONCILE_ENDPOINTS = {
    "market_data": "/api/operator/market-data-projection-reconcile/run-once",
    "market_reference": "/api/operator/market-reference-projection-reconcile/run-once",
    "market_index": "/api/operator/market-index-projection-reconcile/run-once",
    "market_regime": "/api/operator/market-regime-projection-reconcile/run-once",
    "market_scan": "/api/operator/market-scan-projection-reconcile/run-once",
}
_RECONCILE_MIN_TIMEOUT_SEC = {"market_scan": 120.0}
Fetch = Callable[..., dict[str, Any]]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Keep append-only controller/reconcile evidence fresh in OBSERVE-safe mode."
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
    parser.add_argument("--trade-date", default=_today_kst())
    parser.add_argument("--interval-sec", type=float, default=180.0)
    parser.add_argument("--reconcile-limit", type=int, default=500)
    parser.add_argument("--outbox-limit", type=int, default=100)
    parser.add_argument("--outbox-max-batches", type=int, default=5)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--stop-file", default="")
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "append_only_evidence_keeper"),
    )
    args = parser.parse_args()
    if not args.token.strip():
        print("append-only evidence keeper: FAIL local Core token is required")
        return 2

    stop_event = threading.Event()

    def _stop(_signum, _frame) -> None:
        stop_event.set()

    for name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), _stop)

    cycle_kwargs = {
        "core_url": args.core_url,
        "token": args.token,
        "expected_db_path": os.path.abspath(args.expected_db_path),
        "trade_date": args.trade_date,
        "reconcile_limit": min(max(int(args.reconcile_limit), 1), 5000),
        "outbox_limit": min(max(int(args.outbox_limit), 1), 500),
        "outbox_max_batches": min(max(int(args.outbox_max_batches), 1), 20),
        "timeout_sec": max(float(args.timeout_sec), 1.0),
    }
    out_dir = Path(args.out_dir)
    if args.once:
        report = run_keeper_cycle(**cycle_kwargs)
        write_keeper_record(report, out_dir=out_dir)
        print(render_console_summary(report))
        return 2 if report["verdict"]["status"] == "FAIL_SAFETY" else 0

    interval_sec = max(float(args.interval_sec), 30.0)
    stop_file = Path(args.stop_file).resolve() if args.stop_file else None
    while not stop_event.is_set():
        if stop_file is not None and stop_file.exists():
            break
        started = time.monotonic()
        try:
            report = run_keeper_cycle(**cycle_kwargs)
        except Exception as exc:
            report = {
                "format": "append-only-evidence-keeper/v1",
                "generated_at": _now(),
                "trade_date": args.trade_date,
                "verdict": {
                    "status": "WARN",
                    "failures": [],
                    "warnings": ["KEEPER_CYCLE_EXCEPTION"],
                    "error": str(exc),
                    "no_trading_side_effects": True,
                },
            }
        write_keeper_record(report, out_dir=out_dir)
        print(render_console_summary(report), flush=True)
        if report["verdict"]["status"] == "FAIL_SAFETY":
            return 2
        wait_sec = max(interval_sec - (time.monotonic() - started), 15.0)
        _wait_for_stop(stop_event, stop_file=stop_file, wait_sec=wait_sec)
    return 0


def run_keeper_cycle(
    *,
    core_url: str,
    token: str,
    expected_db_path: str,
    trade_date: str,
    reconcile_limit: int,
    outbox_limit: int,
    outbox_max_batches: int,
    timeout_sec: float,
    fetcher: Fetch = fetch_json,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    report: dict[str, Any] = {
        "format": "append-only-evidence-keeper/v1",
        "generated_at": _now(),
        "trade_date": trade_date,
        "core_url": base_url,
        "expected_db_path": os.path.abspath(expected_db_path),
        "core_status": _fetch(
            fetcher, base_url, "/api/status", token, timeout_sec
        ),
        "command_status_before": _fetch(
            fetcher, base_url, "/api/gateway/commands/status", token, timeout_sec
        ),
    }
    preflight_failures = evaluate_keeper_preflight(report)
    report["preflight"] = {
        "status": "PASS" if not preflight_failures else "FAIL",
        "failures": preflight_failures,
        "mutating_actions_allowed": not preflight_failures,
    }
    if preflight_failures:
        report["verdict"] = {
            "status": "FAIL_SAFETY",
            "failures": preflight_failures,
            "warnings": [],
            "no_trading_side_effects": True,
        }
        return report

    report["projection_outbox_drain"] = _drain_projection_outbox(
        fetcher=fetcher,
        base_url=base_url,
        token=token,
        limit=outbox_limit,
        max_batches=outbox_max_batches,
        timeout_sec=timeout_sec,
    )
    report["lifecycle_run"] = _fetch(
        fetcher,
        base_url,
        "/api/operator/live-sim/lifecycle-consumer/run-once?limit=100",
        token,
        timeout_sec,
        method="POST",
    )
    report["reconcile_runs"] = {}
    report["reconcile_runs"]["market_data"] = _run_reconcile(
        fetcher=fetcher,
        base_url=base_url,
        token=token,
        component="market_data",
        limit=reconcile_limit,
        timeout_sec=timeout_sec,
    )
    report["controller_snapshot"] = _fetch(
        fetcher,
        base_url,
        "/api/operator/market-data-append-only/controller/snapshot",
        token,
        timeout_sec,
        method="POST",
    )
    for component in (
        "market_reference",
        "market_index",
        "market_regime",
        "market_scan",
    ):
        report["reconcile_runs"][component] = _run_reconcile(
            fetcher=fetcher,
            base_url=base_url,
            token=token,
            component=component,
            limit=reconcile_limit,
            timeout_sec=timeout_sec,
        )
    report["controller_status"] = _fetch(
        fetcher,
        base_url,
        "/api/operator/market-data-append-only/controller/status",
        token,
        timeout_sec,
    )
    report["routing_status"] = _fetch(
        fetcher,
        base_url,
        "/api/operator/market-data-append-only-routing/status",
        token,
        timeout_sec,
    )
    report["projection_outbox_status"] = _fetch(
        fetcher,
        base_url,
        "/api/operator/projection-outbox/status",
        token,
        timeout_sec,
    )
    report["command_status_after"] = _fetch(
        fetcher,
        base_url,
        "/api/gateway/commands/status",
        token,
        timeout_sec,
    )
    report["core_status_after"] = _fetch(
        fetcher, base_url, "/api/status", token, timeout_sec
    )
    report["verdict"] = evaluate_keeper_report(report)
    return report


def evaluate_keeper_preflight(report: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    for key in ("core_status", "command_status_before"):
        if not _api_ok(report.get(key)):
            failures.append(f"{key.upper()}_API_ERROR")
    core = _data(report, "core_status")
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
    commands = _data(report, "command_status_before")
    if int(commands.get("order_command_count") or 0) != 0:
        failures.append("ORDER_COMMAND_BASELINE_NONZERO")
    return sorted(set(failures))


def evaluate_keeper_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in (
        "lifecycle_run",
        "controller_snapshot",
        "controller_status",
        "routing_status",
        "projection_outbox_status",
        "command_status_after",
        "core_status_after",
    ):
        if not _api_ok(report.get(key)):
            warnings.append(f"{key.upper()}_API_ERROR")
    for component, payload in _mapping(report.get("reconcile_runs")).items():
        if not _api_ok(payload):
            warnings.append(f"{str(component).upper()}_RECONCILE_API_ERROR")
            continue
        reconcile = _data_value(payload)
        if str(reconcile.get("status") or "") != "PASS" or reconcile.get(
            "append_only_ready"
        ) is not True:
            warnings.append(f"{str(component).upper()}_RECONCILE_NOT_PASS")

    drain = _mapping(report.get("projection_outbox_drain"))
    if drain.get("status") != "COMPLETED":
        warnings.append("PROJECTION_OUTBOX_DRAIN_INCOMPLETE")
    if int(drain.get("error_count") or 0):
        failures.append("PROJECTION_OUTBOX_DRAIN_ERROR")
    if int(drain.get("dead_letter_count") or 0):
        failures.append("PROJECTION_OUTBOX_DRAIN_DEAD_LETTER")

    outbox = _data(report, "projection_outbox_status")
    if int(outbox.get("error_count") or 0):
        failures.append("PROJECTION_OUTBOX_ERROR")
    if int(outbox.get("dead_letter_count") or 0):
        failures.append("PROJECTION_OUTBOX_DEAD_LETTER")
    controller = _data(report, "controller_status")
    if str(controller.get("status") or "") != "PASS":
        warnings.append("CONTROLLER_NOT_PASS")
    if bool(controller.get("auto_rollback_required")):
        failures.append("CONTROLLER_AUTO_ROLLBACK_REQUIRED")
    routing = _data(report, "routing_status")
    if int(routing.get("condition_event_candidate_ingest_executed_count") or 0):
        failures.append("CONDITION_EVENT_CANDIDATE_INGEST_EXECUTED")
    if int(routing.get("invalid_effective_skip_count") or 0):
        failures.append("INVALID_EFFECTIVE_SKIP")

    core_after = _data(report, "core_status_after")
    if str(core_after.get("profile") or "") != "OBSERVE" or str(
        core_after.get("mode") or ""
    ) != "OBSERVE":
        failures.append("CORE_LEFT_OBSERVE_DURING_CYCLE")
    if bool(core_after.get("live_sim_allowed")) or bool(
        core_after.get("live_real_allowed")
    ):
        failures.append("LIVE_TRADING_ENABLED_DURING_CYCLE")

    before = _data(report, "command_status_before")
    after = _data(report, "command_status_after")
    before_orders = int(before.get("order_command_count") or 0)
    after_orders = int(after.get("order_command_count") or 0)
    if after_orders != before_orders or after_orders != 0:
        failures.append("ORDER_COMMAND_COUNT_CHANGED")
    safety_failures = {
        "CONDITION_EVENT_CANDIDATE_INGEST_EXECUTED",
        "CORE_LEFT_OBSERVE_DURING_CYCLE",
        "INVALID_EFFECTIVE_SKIP",
        "LIVE_TRADING_ENABLED_DURING_CYCLE",
        "ORDER_COMMAND_COUNT_CHANGED",
    }
    status = (
        "FAIL_SAFETY"
        if safety_failures.intersection(failures)
        else "FAIL"
        if failures
        else "WARN"
        if warnings
        else "PASS"
    )
    return {
        "status": status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "order_command_count_delta": after_orders - before_orders,
        "condition_event_effective_skip_count": int(
            routing.get("condition_event_effective_skip_count") or 0
        ),
        "candidate_ingest_executed_count": int(
            routing.get("condition_event_candidate_ingest_executed_count") or 0
        ),
        "no_trading_side_effects": after_orders == before_orders == 0,
    }


def write_keeper_record(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    trade_date = str(report.get("trade_date") or "unknown")
    report_dir = out_dir / trade_date
    report_dir.mkdir(parents=True, exist_ok=True)
    journal_path = report_dir / "keeper.jsonl"
    latest_path = report_dir / "latest.json"
    serialized = json.dumps(report, ensure_ascii=False, sort_keys=True)
    with journal_path.open("a", encoding="utf-8") as handle:
        handle.write(serialized + "\n")
    temporary = latest_path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, latest_path)
    return {"journal": journal_path, "latest": latest_path}


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "append-only evidence keeper: "
        f"{verdict.get('status')} date={report.get('trade_date')} "
        f"condition_skip={verdict.get('condition_event_effective_skip_count', 0)} "
        f"order_delta={verdict.get('order_command_count_delta', 0)}"
    )


def _drain_projection_outbox(
    *,
    fetcher: Fetch,
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
    for _ in range(max_batches):
        payload = _fetch(
            fetcher,
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
        if not _api_ok(payload) or totals["remaining_pending_count"] == 0:
            break
    totals["status"] = (
        "COMPLETED" if totals["remaining_pending_count"] == 0 else "INCOMPLETE"
    )
    totals["no_trading_side_effects"] = True
    return totals


def _run_reconcile(
    *,
    fetcher: Fetch,
    base_url: str,
    token: str,
    component: str,
    limit: int,
    timeout_sec: float,
) -> dict[str, Any]:
    endpoint = _RECONCILE_ENDPOINTS[component]
    path = endpoint + "?" + urllib.parse.urlencode(
        {"limit": limit, "persist": "true", "live_safe": "true"}
    )
    timeout = max(
        timeout_sec,
        _RECONCILE_MIN_TIMEOUT_SEC.get(component, timeout_sec),
    )
    primary = _fetch(
        fetcher,
        base_url,
        path,
        token,
        timeout,
        method="POST",
    )
    if not is_locked_retryable_payload(primary):
        return primary
    fallback_path = endpoint + "?" + urllib.parse.urlencode(
        {"limit": limit, "persist": "false", "live_safe": "true"}
    )
    fallback = _fetch(
        fetcher,
        base_url,
        fallback_path,
        token,
        timeout,
        method="POST",
    )
    fallback["persist_fallback"] = {
        "used": True,
        "reason_code": "SQLITE_DATABASE_LOCKED",
        "primary_status_code": primary.get("status_code"),
    }
    return fallback


def _fetch(
    fetcher: Fetch,
    base_url: str,
    path: str,
    token: str,
    timeout_sec: float,
    *,
    method: str = "GET",
) -> dict[str, Any]:
    return fetcher(
        f"{base_url}{path}",
        token=token,
        method=method,
        timeout_sec=timeout_sec,
    )


def _api_ok(value: object) -> bool:
    return isinstance(value, Mapping) and bool(value.get("ok"))


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    return _data_value(report.get(key))


def _data_value(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    data = value.get("data", value)
    return dict(data) if isinstance(data, Mapping) else {}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _normalize_path(value: object) -> str:
    if value in (None, ""):
        return ""
    return os.path.normcase(os.path.abspath(os.fspath(value)))


def _today_kst() -> str:
    return datetime.now(KST).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _wait_for_stop(
    stop_event: threading.Event,
    *,
    stop_file: Path | None,
    wait_sec: float,
) -> None:
    deadline = time.monotonic() + max(wait_sec, 0.0)
    while not stop_event.is_set() and time.monotonic() < deadline:
        if stop_file is not None and stop_file.exists():
            stop_event.set()
            return
        stop_event.wait(min(1.0, max(deadline - time.monotonic(), 0.0)))


if __name__ == "__main__":
    raise SystemExit(main())
