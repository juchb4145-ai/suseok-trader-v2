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
        description="Check incremental evaluation backlog, age, and dead-letter health."
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
    parser.add_argument(
        "--require-effective-clear",
        action="store_true",
        help=(
            "Require the append-only effective dead-letter view to be clear. "
            "Raw historical rows remain visible and are not reset."
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "incremental_evaluation_queue"),
    )
    args = parser.parse_args()
    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
        require_effective_clear=args.require_effective_clear,
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    out_dir: Path,
    require_effective_clear: bool = False,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "incremental_evaluation,pipeline_summary",
            "timeout_budget_ms": "5000",
        }
    )
    report: dict[str, Any] = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "core_url": base_url,
        "requested_contract": {
            "require_effective_clear": bool(require_effective_clear),
        },
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
        "queue_status_before": fetch_json(
            f"{base_url}/api/operator/incremental-evaluation/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "dead_letters_before": fetch_json(
            f"{base_url}/api/operator/incremental-evaluation/dead-letters?limit=100",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
    }
    report.update(
        {
            "queue_status_after": fetch_json(
                f"{base_url}/api/operator/incremental-evaluation/status",
                token=token,
                method="GET",
                timeout_sec=timeout_sec,
            ),
            "dead_letters_after": fetch_json(
                f"{base_url}/api/operator/incremental-evaluation/dead-letters?limit=100",
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
    )
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    required = (
        "core_status",
        "command_status_before",
        "queue_status_before",
        "dead_letters_before",
        "queue_status_after",
        "dead_letters_after",
        "dashboard_snapshot",
        "command_status_after",
    )
    for key in required:
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core = _data(report, "core_status")
    before_commands = _data(report, "command_status_before")
    after_commands = _data(report, "command_status_after")
    before = _data(report, "queue_status_before")
    after = _data(report, "queue_status_after")
    dead_after = _data(report, "dead_letters_after")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_status = dashboard.get("incremental_evaluation")
    dashboard_status = dashboard_status if isinstance(dashboard_status, Mapping) else {}
    contract = _mapping(report.get("requested_contract"))
    require_effective_clear = bool(contract.get("require_effective_clear"))

    if core.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if bool(core.get("live_sim_allowed")) or bool(core.get("live_real_allowed")):
        failures.append("LIVE_TRADING_ALLOWED")
    command_delta = _command_count(after_commands) - _command_count(before_commands)
    order_command_delta = int(after_commands.get("order_command_count") or 0) - int(
        before_commands.get("order_command_count") or 0
    )
    if command_delta:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_delta:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")
    if bool(before_commands.get("order_commands_allowed")) or bool(
        after_commands.get("order_commands_allowed")
    ):
        failures.append("ORDER_COMMANDS_ALLOWED")

    final_status = str(after.get("status") or "")
    effective_fields = _effective_dead_letter_fields(after)
    effective_contract_valid = effective_fields is not None
    if require_effective_clear:
        if not effective_contract_valid:
            failures.append("INCREMENTAL_EFFECTIVE_STATUS_CONTRACT_MISSING")
        else:
            assert effective_fields is not None
            if effective_fields["effective_status"] not in {"PASS", "WARN", "CLEAR"}:
                failures.append("INCREMENTAL_EFFECTIVE_STATUS_FAIL")
            if effective_fields["fast_0_status"] != "CLEAR":
                failures.append("INCREMENTAL_FAST_0_NOT_CLEAR")
            if effective_fields["effective_dead_letter_count"]:
                failures.append("INCREMENTAL_EFFECTIVE_DEAD_LETTER_PRESENT")
            if effective_fields["active_unresolved_dead_letter_count"]:
                failures.append("INCREMENTAL_ACTIVE_DEAD_LETTER_PRESENT")
            if effective_fields["historical_pending_disposition_count"]:
                failures.append("INCREMENTAL_HISTORICAL_DISPOSITION_PENDING")
            if effective_fields["manual_review_dead_letter_count"]:
                failures.append("INCREMENTAL_DEAD_LETTER_MANUAL_REVIEW_REQUIRED")
            if effective_fields["invalid_disposition_count"]:
                failures.append("INCREMENTAL_INVALID_DISPOSITION_PRESENT")
            if effective_fields["raw_dead_letter_count"]:
                warnings.append("INCREMENTAL_HISTORICAL_DEAD_LETTER_PRESERVED")
        reason_codes = {
            str(code) for code in after.get("reason_codes") or [] if str(code)
        }
        non_raw_failure_codes = reason_codes - {
            "INCREMENTAL_QUEUE_DEAD_LETTER_PRESENT",
            "INCREMENTAL_HISTORICAL_DEAD_LETTER_PRESERVED",
        }
        if final_status == "FAIL" and (not reason_codes or non_raw_failure_codes):
            failures.append("INCREMENTAL_QUEUE_STATUS_FAIL")
        elif final_status == "WARN":
            warnings.extend(sorted(reason_codes))
    else:
        if final_status == "FAIL":
            failures.append("INCREMENTAL_QUEUE_STATUS_FAIL")
        elif final_status == "WARN":
            warnings.extend(str(code) for code in after.get("reason_codes") or [])
    if int(after.get("retry_exhausted_count") or 0):
        failures.append("INCREMENTAL_RETRY_EXHAUSTED_ACTIVE")
    if not require_effective_clear and int(after.get("dead_letter_count") or 0):
        failures.append("INCREMENTAL_DEAD_LETTER_PRESENT")
    if int(dead_after.get("count") or 0) != int(after.get("dead_letter_count") or 0):
        failures.append("INCREMENTAL_DEAD_LETTER_LIST_MISMATCH")
    if int(before.get("queued_count") or 0) != int(after.get("queued_count") or 0):
        failures.append("INCREMENTAL_QUEUE_CHANGED_DURING_CHECK")
    if int(before.get("dead_letter_count") or 0) != int(
        after.get("dead_letter_count") or 0
    ):
        failures.append("INCREMENTAL_DEAD_LETTER_CHANGED_DURING_CHECK")
    if require_effective_clear:
        before_effective = _effective_dead_letter_fields(before)
        if before_effective is None:
            failures.append("INCREMENTAL_EFFECTIVE_STATUS_BEFORE_CONTRACT_MISSING")
        elif effective_fields is not None and before_effective != effective_fields:
            failures.append("INCREMENTAL_EFFECTIVE_STATUS_CHANGED_DURING_CHECK")
    if not dashboard_status:
        failures.append("DASHBOARD_INCREMENTAL_STATUS_MISSING")
    elif int(dashboard_status.get("queued_count") or 0) != int(
        after.get("queued_count") or 0
    ):
        failures.append("DASHBOARD_INCREMENTAL_STATUS_MISMATCH")

    verdict_status = "FAIL" if failures else "WARN" if warnings else "PASS"
    effective_after = effective_fields or {}
    return {
        "status": verdict_status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "queued_before": int(before.get("queued_count") or 0),
        "queued_after": int(after.get("queued_count") or 0),
        "stale_after": int(after.get("stale_queue_count") or 0),
        "dead_letter_after": int(after.get("dead_letter_count") or 0),
        "raw_dead_letter_after": int(
            effective_after.get("raw_dead_letter_count")
            or after.get("dead_letter_count")
            or 0
        ),
        "effective_dead_letter_after": (
            effective_after.get("effective_dead_letter_count")
        ),
        "historical_disposed_after": effective_after.get(
            "historical_disposed_dead_letter_count"
        ),
        "historical_pending_after": effective_after.get(
            "historical_pending_disposition_count"
        ),
        "manual_review_after": effective_after.get(
            "manual_review_dead_letter_count"
        ),
        "invalid_disposition_after": effective_after.get(
            "invalid_disposition_count"
        ),
        "fast_0_status": effective_after.get("fast_0_status"),
        "require_effective_clear": require_effective_clear,
        "effective_contract_valid": effective_contract_valid,
        "command_count_delta": command_delta,
        "order_command_count_delta": order_command_delta,
        "read_only_by_default": True,
        "no_order_side_effects": True,
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
            "# Incremental Evaluation Queue Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            (
                "- queued_before/after: "
                f"`{verdict.get('queued_before')}/{verdict.get('queued_after')}`"
            ),
            f"- stale_after: `{verdict.get('stale_after')}`",
            f"- dead_letter_after: `{verdict.get('dead_letter_after')}`",
            (
                "- effective_dead_letter_after: "
                f"`{verdict.get('effective_dead_letter_after')}`"
            ),
            f"- fast_0_status: `{verdict.get('fast_0_status')}`",
            (
                "- require_effective_clear: "
                f"`{verdict.get('require_effective_clear')}`"
            ),
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            f"- order_command_count_delta: `{verdict.get('order_command_count_delta')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "The default check is read-only and never creates an order command.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "Incremental evaluation queue: "
        f"{verdict.get('status')} queued={verdict.get('queued_after')} "
        f"stale={verdict.get('stale_after')} "
        f"dead_letter={verdict.get('dead_letter_after')}"
    )


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


def _effective_dead_letter_fields(status: Mapping[str, Any]) -> dict[str, Any] | None:
    integer_fields = (
        "raw_dead_letter_count",
        "effective_dead_letter_count",
        "active_unresolved_dead_letter_count",
        "historical_pending_disposition_count",
        "historical_disposed_dead_letter_count",
        "manual_review_dead_letter_count",
        "invalid_disposition_count",
    )
    parsed: dict[str, Any] = {}
    for name in integer_fields:
        value = status.get(name)
        if not isinstance(value, int) or isinstance(value, bool):
            return None
        parsed[name] = value
        if value < 0:
            return None
    for name in ("raw_status", "effective_status", "fast_0_status"):
        value = status.get(name)
        if not isinstance(value, str) or not value.strip():
            return None
        parsed[name] = value.strip().upper()
    raw_count = parsed["raw_dead_letter_count"]
    legacy_raw_count = status.get("dead_letter_count")
    if not isinstance(legacy_raw_count, int) or isinstance(legacy_raw_count, bool):
        return None
    if raw_count != legacy_raw_count:
        return None
    classified_count = (
        parsed["effective_dead_letter_count"]
        + parsed["historical_disposed_dead_letter_count"]
    )
    if classified_count != raw_count:
        return None
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
