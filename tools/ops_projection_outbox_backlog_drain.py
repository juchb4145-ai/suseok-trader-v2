from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tools.ops_market_data_tr_response_side_effect_check import (
    fetch_json,
    is_locked_retryable_payload,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check and optionally drain projection_outbox backlog readiness."
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
    parser.add_argument("--drain", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-batches", type=int, default=1)
    parser.add_argument("--sleep-sec", type=float, default=0.5)
    parser.add_argument("--stop-on-fail", action="store_true")
    parser.add_argument("--stop-on-locked", action="store_true")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "projection_outbox_backlog"),
    )
    args = parser.parse_args()

    report = run_backlog_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        drain=args.drain,
        limit=args.limit,
        max_batches=args.max_batches,
        sleep_sec=args.sleep_sec,
        stop_on_fail=args.stop_on_fail,
        stop_on_locked=args.stop_on_locked,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_backlog_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    drain: bool,
    limit: int,
    max_batches: int,
    sleep_sec: float,
    stop_on_fail: bool,
    stop_on_locked: bool,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    generated_at = (
        datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    initial_backlog = _fetch_backlog(
        base_url,
        token=token,
        timeout_sec=timeout_sec,
    )
    drain_payloads: list[dict[str, Any]] = []
    locked_observed = False
    if drain:
        for batch_index in range(max(int(max_batches), 0)):
            payload = _drain_once(
                base_url,
                token=token,
                timeout_sec=timeout_sec,
                limit=limit,
                stop_on_locked=stop_on_locked,
            )
            payload["batch_index"] = batch_index
            drain_payloads.append(payload)
            locked_observed = locked_observed or is_locked_retryable_payload(payload)
            if locked_observed and stop_on_locked:
                break
            if _drain_payload_status(payload) == "LOCKED_RETRYABLE" and stop_on_locked:
                break
            if stop_on_fail:
                interim = _fetch_backlog(
                    base_url,
                    token=token,
                    timeout_sec=timeout_sec,
                )
                if _backlog_status(interim) == "FAIL":
                    break
            if sleep_sec > 0 and batch_index < max_batches - 1:
                time.sleep(sleep_sec)

    final_backlog = _fetch_backlog(base_url, token=token, timeout_sec=timeout_sec)
    latest_reconcile = fetch_json(
        f"{base_url}/api/operator/market-data-projection-reconcile/latest",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    report = {
        "generated_at": generated_at,
        "core_url": base_url,
        "drain_requested": bool(drain),
        "limit": int(limit),
        "max_batches": int(max_batches),
        "initial_backlog": initial_backlog,
        "drain_payloads": drain_payloads,
        "final_backlog": final_backlog,
        "latest_reconcile": latest_reconcile,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in ("initial_backlog", "final_backlog", "latest_reconcile"):
        payload = report.get(key) or {}
        if not payload.get("ok"):
            failures.append(f"{key.upper()}_API_ERROR")

    final = _data(report, "final_backlog")
    initial = _data(report, "initial_backlog")
    final_status = str(final.get("readiness_status") or "").upper()
    if final_status == "FAIL":
        failures.append("PROJECTION_OUTBOX_BACKLOG_READINESS_FAIL")
    elif final_status == "WARN":
        warnings.append("PROJECTION_OUTBOX_BACKLOG_READINESS_WARN")
    if int(final.get("error_count") or 0) > 0:
        failures.append("PROJECTION_OUTBOX_ERROR")
    if int(final.get("dead_letter_count") or 0) > 0:
        failures.append("PROJECTION_OUTBOX_DEAD_LETTER")
    if int(final.get("stale_processing_count") or 0) > 0:
        failures.append("STALE_OUTBOX_PROCESSING")
    if not bool(final.get("pr11_condition_event_cutover_ready")):
        warnings.append("PR11_CONDITION_EVENT_CUTOVER_NOT_READY")

    latest_run = _data(report, "latest_reconcile").get("latest_run")
    if isinstance(latest_run, dict) and latest_run.get("status") == "FAIL":
        failures.append("LATEST_RECONCILE_FAIL")
    elif not isinstance(latest_run, dict):
        warnings.append("LATEST_RECONCILE_MISSING")

    drain_payloads = [
        payload for payload in report.get("drain_payloads", []) if isinstance(payload, dict)
    ]
    locked_or_partial = False
    for payload in drain_payloads:
        status = _drain_payload_status(payload)
        if is_locked_retryable_payload(payload) or status == "LOCKED_RETRYABLE":
            warnings.append("PROJECTION_OUTBOX_DRAIN_LOCKED_RETRYABLE")
            locked_or_partial = True
        if status == "PARTIAL":
            warnings.append("PROJECTION_OUTBOX_DRAIN_PARTIAL")
            locked_or_partial = True
        if not payload.get("ok"):
            failures.append("PROJECTION_OUTBOX_DRAIN_API_ERROR")
    if bool(report.get("drain_requested")) and not locked_or_partial:
        before = int(initial.get("pending_count") or initial.get("total_pending_count") or 0)
        after = int(final.get("pending_count") or final.get("total_pending_count") or 0)
        if before > 0 and after >= before:
            failures.append("PROJECTION_OUTBOX_PENDING_NOT_DECREASED")
        elif before > after and after > 0:
            warnings.append("PROJECTION_OUTBOX_PENDING_DECREASED_BUT_REMAINS")

    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    block_next_pr = bool(
        failures
        or final_status == "FAIL"
        or not bool(final.get("pr11_condition_event_cutover_ready"))
    )
    return {
        "status": status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": block_next_pr,
        "pr11_condition_event_cutover_ready": bool(
            final.get("pr11_condition_event_cutover_ready")
        ),
    }


def write_report(report: dict[str, Any], *, out_dir: Path) -> dict[str, Path]:
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


def render_markdown_summary(report: dict[str, Any]) -> str:
    final = _data(report, "final_backlog")
    verdict = report.get("verdict", {})
    lines = [
        "# Projection Outbox Backlog Drain",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- block_next_pr: `{verdict.get('block_next_pr')}`",
        f"- readiness_status: `{final.get('readiness_status')}`",
        f"- pr11_condition_event_cutover_ready: `{final.get('pr11_condition_event_cutover_ready')}`",
        f"- pending_count: `{final.get('pending_count')}`",
        f"- recent_pending_count: `{final.get('recent_pending_count')}`",
        f"- condition_event_pending_count: `{final.get('condition_event_pending_count')}`",
        f"- stale_processing_count: `{final.get('stale_processing_count')}`",
        f"- error_count: `{final.get('error_count')}`",
        f"- dead_letter_count: `{final.get('dead_letter_count')}`",
        f"- operator_actions: `{final.get('operator_actions')}`",
        f"- failures: `{verdict.get('failures', [])}`",
        f"- warnings: `{verdict.get('warnings', [])}`",
        "",
        "## Safety",
        "",
        "- This script does not enable condition_event cutover.",
        "- drain-once keeps existing worker apply settings.",
        "- LIVE_REAL/order/safety gates are unchanged.",
    ]
    return "\n".join(lines) + "\n"


def render_console_summary(report: dict[str, Any]) -> str:
    final = _data(report, "final_backlog")
    verdict = report.get("verdict", {})
    return (
        "projection_outbox backlog: "
        f"{verdict.get('status')} "
        f"readiness={final.get('readiness_status')} "
        f"pending={final.get('pending_count')} "
        f"recent={final.get('recent_pending_count')} "
        f"condition_pending={final.get('condition_event_pending_count')} "
        f"pr11_ready={final.get('pr11_condition_event_cutover_ready')}"
    )


def _fetch_backlog(
    base_url: str,
    *,
    token: str,
    timeout_sec: float,
) -> dict[str, Any]:
    return fetch_json(
        f"{base_url}/api/operator/projection-outbox/backlog",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )


def _drain_once(
    base_url: str,
    *,
    token: str,
    timeout_sec: float,
    limit: int,
    stop_on_locked: bool,
) -> dict[str, Any]:
    params = urllib.parse.urlencode(
        {
            "limit": str(limit),
            "apply_projection": "true",
            "live_safe": "true",
            "max_batches": "1",
            "stop_on_locked": "true" if stop_on_locked else "false",
        }
    )
    return fetch_json(
        f"{base_url}/api/operator/projection-outbox/drain-once?{params}",
        token=token,
        method="POST",
        timeout_sec=timeout_sec,
    )


def _backlog_status(payload: dict[str, Any]) -> str:
    return str(_payload_data(payload).get("readiness_status") or "").upper()


def _drain_payload_status(payload: dict[str, Any]) -> str:
    return str(_payload_data(payload).get("status") or "").upper()


def _data(report: dict[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, dict):
        return {}
    return _payload_data(payload)


def _payload_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
