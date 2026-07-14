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
        description=(
            "Check projection success/error watermarks and retention RCA without "
            "deleting events or routing orders."
        )
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
    parser.add_argument("--retention-days", type=int)
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--evidence-event-id")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "projection_retention"),
    )
    args = parser.parse_args()
    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        retention_days=args.retention_days,
        sample_limit=args.sample_limit,
        evidence_event_id=args.evidence_event_id,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    retention_days: int | None,
    sample_limit: int,
    evidence_event_id: str | None,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    retention_query = urllib.parse.urlencode(
        {
            key: value
            for key, value in {
                "retention_days": retention_days,
                "exact_counts": "true",
            }.items()
            if value is not None
        }
    )
    rca_query = urllib.parse.urlencode(
        {
            "blocked_only": "false",
            "limit": min(max(int(sample_limit), 1), 100),
            **({"retention_days": retention_days} if retention_days else {}),
        }
    )
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "projection_watermarks,projection_retention,errors",
            "timeout_budget_ms": "5000",
        }
    )
    report = {
        "generated_at": _now(),
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
        "watermark_status": fetch_json(
            f"{base_url}/api/operator/projection-watermarks/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "retention_status": fetch_json(
            f"{base_url}/api/operator/event-retention/status"
            + (f"?{retention_query}" if retention_query else ""),
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "retention_rca": fetch_json(
            f"{base_url}/api/operator/projection-retention/rca?{rca_query}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "backfill_dry_run": fetch_json(
            f"{base_url}/api/operator/projection-watermarks/backfill"
            "?dry_run=true&limit=100",
            token=token,
            method="POST",
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
    if evidence_event_id:
        event_query = urllib.parse.urlencode({"event_id": evidence_event_id})
        report["evidence_event_rca"] = fetch_json(
            f"{base_url}/api/operator/projection-retention/rca?{event_query}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        )
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    api_keys = (
        "core_status",
        "command_status",
        "watermark_status",
        "retention_status",
        "retention_rca",
        "backfill_dry_run",
        "dashboard_snapshot",
        "command_status_after",
    )
    for key in api_keys:
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")
    if "evidence_event_rca" in report:
        evidence_payload = report.get("evidence_event_rca")
        if not isinstance(evidence_payload, Mapping) or not evidence_payload.get(
            "ok",
            True,
        ):
            failures.append("EVIDENCE_EVENT_RCA_API_ERROR")

    core = _data(report, "core_status")
    before = _data(report, "command_status")
    after = _data(report, "command_status_after")
    watermarks = _data(report, "watermark_status")
    retention = _data(report, "retention_status")
    rca = _data(report, "retention_rca")
    evidence_event_rca = _data(report, "evidence_event_rca")
    backfill = _data(report, "backfill_dry_run")
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

    age_eligible = int(retention.get("age_eligible_event_count") or 0)
    eligible = int(retention.get("candidate_event_count") or 0)
    blocked = int(retention.get("projection_blocked_event_count") or 0)
    if retention.get("counts_exact") is not True:
        failures.append("RETENTION_COUNTS_NOT_EXACT")
    if age_eligible != eligible + blocked:
        failures.append("RETENTION_COUNT_CONSERVATION_MISMATCH")
    if bool(retention.get("projection_retention_gate_pass")) != (blocked == 0):
        failures.append("RETENTION_GATE_STATUS_MISMATCH")
    if bool(retention.get("apply_ready")) and not bool(retention.get("enabled")):
        failures.append("RETENTION_APPLY_READY_WHILE_DISABLED")
    if int(watermarks.get("unresolved_error_count") or 0):
        warnings.append("PROJECTION_EVENT_ERRORS_UNRESOLVED")
    if blocked:
        warnings.append("PROJECTION_RETENTION_EVENTS_BLOCKED")
    if not bool(retention.get("enabled")):
        warnings.append("EVENT_RETENTION_DISABLED_SAFE_DEFAULT")
    if not bool(rca.get("read_only")):
        failures.append("RETENTION_RCA_NOT_READ_ONLY")
    if "evidence_event_rca" in report and not bool(evidence_event_rca.get("read_only")):
        failures.append("EVIDENCE_EVENT_RCA_NOT_READ_ONLY")
    if backfill.get("dry_run") is not True or int(backfill.get("applied_count") or 0):
        failures.append("BACKFILL_DRY_RUN_CONTRACT_FAILED")

    dashboard_watermarks = dashboard.get("projection_watermarks")
    dashboard_retention = dashboard.get("projection_retention")
    if not isinstance(dashboard_watermarks, Mapping):
        failures.append("DASHBOARD_PROJECTION_WATERMARKS_MISSING")
    if not isinstance(dashboard_retention, Mapping):
        failures.append("DASHBOARD_PROJECTION_RETENTION_MISSING")
    elif int(dashboard_retention.get("projection_blocked_event_count") or 0) != blocked:
        failures.append("DASHBOARD_RETENTION_COUNT_MISMATCH")

    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "age_eligible_event_count": age_eligible,
        "retention_eligible_event_count": eligible,
        "projection_blocked_event_count": blocked,
        "unresolved_projection_error_count": int(
            watermarks.get("unresolved_error_count") or 0
        ),
        "backfill_candidate_count": int(backfill.get("candidate_count") or 0),
        "rca_venue_counts": dict(rca.get("venue_counts") or {}),
        "evidence_event_venue_counts": dict(
            evidence_event_rca.get("venue_counts") or {}
        ),
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "retention_apply_executed": False,
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
            "# Projection Watermark and Retention Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            (
                "- age_eligible_event_count: "
                f"`{verdict.get('age_eligible_event_count')}`"
            ),
            (
                "- retention_eligible_event_count: "
                f"`{verdict.get('retention_eligible_event_count')}`"
            ),
            (
                "- projection_blocked_event_count: "
                f"`{verdict.get('projection_blocked_event_count')}`"
            ),
            (
                "- unresolved_projection_error_count: "
                f"`{verdict.get('unresolved_projection_error_count')}`"
            ),
            f"- backfill_candidate_count: `{verdict.get('backfill_candidate_count')}`",
            (
                "- rca_venue_counts: `"
                f"{json.dumps(verdict.get('rca_venue_counts') or {}, sort_keys=True)}`"
            ),
            (
                "- evidence_event_venue_counts: `"
                f"{json.dumps(verdict.get('evidence_event_venue_counts') or {}, sort_keys=True)}`"
            ),
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            (
                "- order_command_count_delta: "
                f"`{verdict.get('order_command_count_delta')}`"
            ),
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "This check performs only GET requests and a projection-result backfill dry-run. "
            "It never executes retention apply or creates an order command.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return (
        "Projection retention: "
        f"{verdict.get('status')} eligible={verdict.get('retention_eligible_event_count')} "
        f"blocked={verdict.get('projection_blocked_event_count')} "
        f"errors={verdict.get('unresolved_projection_error_count')}"
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


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
