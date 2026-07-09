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
        description="Check PR-13 market_reference projection readiness."
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
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_reference_projection"),
    )
    args = parser.parse_args()

    report = run_market_reference_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_market_reference_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    timeout_sec: float,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_params = {
        "sections": (
            "gateway,market_data,market_reference,projection_outbox,"
            "pipeline_summary,errors"
        ),
        "detail": "summary",
        "limit": "20",
        "fast": "true",
        "timeout_budget_ms": "5000",
    }
    report = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "core_url": base_url,
        "reconcile_run": fetch_json(
            f"{base_url}/api/operator/market-reference-projection-reconcile/run-once"
            f"?{urllib.parse.urlencode({'limit': limit, 'persist': 'true', 'live_safe': 'true'})}",
            token=token,
            method="POST",
            timeout_sec=timeout_sec,
        ),
        "latest_reconcile": fetch_json(
            f"{base_url}/api/operator/market-reference-projection-reconcile/latest",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "routing_status": fetch_json(
            f"{base_url}/api/operator/market-reference-append-only-routing/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "market_reference_status": fetch_json(
            f"{base_url}/api/operator/market-reference/status",
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
            f"{base_url}/api/dashboard/snapshot?{urllib.parse.urlencode(dashboard_params)}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in (
        "reconcile_run",
        "latest_reconcile",
        "routing_status",
        "market_reference_status",
        "projection_outbox",
        "dashboard_snapshot",
    ):
        payload = report.get(key) or {}
        if not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    reconcile_run = _data(report, "reconcile_run")
    latest_reconcile = _data(report, "latest_reconcile")
    routing = _data(report, "routing_status")
    reference_status = _data(report, "market_reference_status")
    projection_outbox = _data(report, "projection_outbox")
    dashboard = _data(report, "dashboard_snapshot")
    latest_run = (
        latest_reconcile.get("latest_run")
        if isinstance(latest_reconcile, Mapping)
        else None
    )
    run_payload = latest_run if isinstance(latest_run, Mapping) else reconcile_run

    membership_count = int(
        reference_status.get("membership_count")
        or run_payload.get("stored_membership_count")
        or 0
    )
    missing_membership_count = int(
        reference_status.get("missing_membership_count")
        or run_payload.get("missing_membership_count")
        or 0
    )
    outbox_error_count = int(
        run_payload.get("outbox_error_count")
        or projection_outbox.get("by_projection_name", {})
        .get("market_reference", {})
        .get("error_count", 0)
        or 0
    )
    outbox_dead_letter_count = int(
        run_payload.get("outbox_dead_letter_count")
        or projection_outbox.get("by_projection_name", {})
        .get("market_reference", {})
        .get("dead_letter_count", 0)
        or 0
    )
    effective_skip_count = int(
        routing.get("effective_skip_inline_count")
        or reference_status.get("effective_skip_inline_count")
        or 0
    )
    reconcile_status = str(run_payload.get("status") or "").upper()
    latest_outbox_job = reference_status.get("latest_outbox_job")
    latest_outbox_job = (
        latest_outbox_job if isinstance(latest_outbox_job, Mapping) else {}
    )
    latest_outbox_metadata = latest_outbox_job.get("metadata")
    latest_outbox_metadata = (
        latest_outbox_metadata
        if isinstance(latest_outbox_metadata, Mapping)
        else {}
    )
    worker_evidence = latest_outbox_metadata.get("last_worker_evidence")
    worker_evidence = worker_evidence if isinstance(worker_evidence, Mapping) else {}
    worker_apply_mode = str(worker_evidence.get("apply_mode") or "")
    worker_apply_result = str(worker_evidence.get("apply_result") or "")
    worker_no_trading_side_effects = bool(
        worker_evidence.get("no_trading_side_effects")
    )

    if membership_count <= 0:
        failures.append("MARKET_REFERENCE_MEMBERSHIP_COUNT_ZERO")
    if missing_membership_count > 0:
        failures.append("MARKET_REFERENCE_MEMBERSHIP_MISSING")
    if outbox_error_count > 0 or outbox_dead_letter_count > 0:
        failures.append("MARKET_REFERENCE_OUTBOX_ERROR_OR_DEAD_LETTER")
    if reconcile_status == "FAIL":
        failures.append("MARKET_REFERENCE_RECONCILE_FAIL")
    if effective_skip_count > 0:
        failures.append("MARKET_REFERENCE_EFFECTIVE_SKIP_FORBIDDEN_IN_PR13")
    if _terminal_artifact_missing(run_payload):
        failures.append("MARKET_REFERENCE_TERMINAL_ARTIFACT_MISSING")
    if not worker_evidence:
        warnings.append("MARKET_REFERENCE_WORKER_EVIDENCE_MISSING")
    else:
        if worker_apply_mode != "MARKET_REFERENCE_APPLY":
            failures.append("MARKET_REFERENCE_WORKER_APPLY_MODE_INVALID")
        if worker_apply_result not in {"APPLIED_BY_VERIFY", "APPLIED_BY_WORKER"}:
            failures.append("MARKET_REFERENCE_WORKER_APPLY_RESULT_INVALID")
        if not worker_no_trading_side_effects:
            failures.append("MARKET_REFERENCE_WORKER_TRADING_SIDE_EFFECT_GUARD_MISSING")

    if reconcile_status == "WARN":
        warnings.append("MARKET_REFERENCE_RECONCILE_WARN")
    if not bool(routing.get("dry_run_enabled")):
        warnings.append("MARKET_REFERENCE_DRY_RUN_DISABLED")
    if int(run_payload.get("outbox_pending_count") or 0) > 0:
        warnings.append("MARKET_REFERENCE_OUTBOX_PENDING_WITHIN_SLA")
    if not bool(run_payload.get("append_only_ready")):
        warnings.append("MARKET_REFERENCE_APPEND_ONLY_NOT_READY")
    dashboard_reference = (
        dashboard.get("market_reference") if isinstance(dashboard, Mapping) else None
    )
    if isinstance(dashboard_reference, Mapping) and int(
        dashboard_reference.get("effective_skip_inline_count") or 0
    ) > 0:
        failures.append("DASHBOARD_MARKET_REFERENCE_EFFECTIVE_SKIP")

    worker_evidence_missing = "MARKET_REFERENCE_WORKER_EVIDENCE_MISSING" in warnings
    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_pr14": bool(failures or worker_evidence_missing),
        "membership_count": membership_count,
        "missing_membership_count": missing_membership_count,
        "outbox_error_count": outbox_error_count,
        "outbox_dead_letter_count": outbox_dead_letter_count,
        "effective_skip_inline_count": effective_skip_count,
        "reconcile_status": reconcile_status,
        "worker_apply_mode": worker_apply_mode or None,
        "worker_apply_result": worker_apply_result or None,
        "worker_no_trading_side_effects": worker_no_trading_side_effects,
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


def render_console_summary(report: dict[str, Any]) -> str:
    verdict = report["verdict"]
    return (
        "market_reference projection: "
        f"{verdict['status']} reconcile={verdict.get('reconcile_status')} "
        f"membership={verdict.get('membership_count')} "
        f"missing={verdict.get('missing_membership_count')} "
        f"effective_skip={verdict.get('effective_skip_inline_count')}"
    )


def render_markdown_summary(report: dict[str, Any]) -> str:
    verdict = report["verdict"]
    return "\n".join(
        [
            "# Market Reference Projection Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- reconcile_status: `{verdict.get('reconcile_status')}`",
            f"- membership_count: `{verdict.get('membership_count')}`",
            f"- missing_membership_count: `{verdict.get('missing_membership_count')}`",
            f"- outbox_error_count: `{verdict.get('outbox_error_count')}`",
            f"- outbox_dead_letter_count: `{verdict.get('outbox_dead_letter_count')}`",
            f"- effective_skip_inline_count: `{verdict.get('effective_skip_inline_count')}`",
            f"- worker_apply_mode: `{verdict.get('worker_apply_mode')}`",
            f"- worker_apply_result: `{verdict.get('worker_apply_result')}`",
            f"- worker_no_trading_side_effects: `{verdict.get('worker_no_trading_side_effects')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "PR-13 is dry-run only; market_symbols inline projection remains enabled.",
        ]
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    if "data" in payload and isinstance(payload["data"], Mapping):
        return dict(payload["data"])
    return dict(payload)


def _terminal_artifact_missing(payload: Mapping[str, Any]) -> bool:
    reason_codes = {str(value) for value in payload.get("reason_codes") or []}
    return "MARKET_REFERENCE_ARTIFACT_MISSING_AFTER_TERMINAL_OUTBOX" in reason_codes


if __name__ == "__main__":
    raise SystemExit(main())
