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
        description="Check runtime execution lease, heartbeat, and fencing health."
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
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "runtime_execution_lock"),
    )
    args = parser.parse_args()

    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "runtime_execution_locks,pipeline_summary",
            "timeout_budget_ms": "5000",
        }
    )
    report = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "core_url": base_url,
        "lock_status": fetch_json(
            f"{base_url}/api/operator/runtime-execution-locks/status",
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
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in ("lock_status", "dashboard_snapshot"):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    status = _data(report, "lock_status")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_status = dashboard.get("runtime_execution_locks")
    dashboard_status = (
        dashboard_status if isinstance(dashboard_status, Mapping) else {}
    )
    if str(status.get("status") or "") == "FAIL":
        failures.append("RUNTIME_EXECUTION_LOCK_STATUS_FAIL")
    if int(status.get("stale_expired_count") or 0) > 0:
        failures.append("RUNTIME_EXECUTION_LOCK_STALE_EXPIRED")
    if int(status.get("expired_owner_alive_count") or 0) > 0:
        warnings.append("RUNTIME_EXECUTION_LOCK_EXPIRED_OWNER_ALIVE")

    locks = status.get("locks")
    locks = locks if isinstance(locks, list) else []
    for lock in locks:
        if not isinstance(lock, Mapping):
            failures.append("RUNTIME_EXECUTION_LOCK_INVALID_PAYLOAD")
            continue
        if int(lock.get("fencing_token") or 0) <= 0:
            failures.append("RUNTIME_EXECUTION_LOCK_FENCING_TOKEN_MISSING")
        if int(lock.get("process_id") or 0) <= 0:
            failures.append("RUNTIME_EXECUTION_LOCK_PROCESS_ID_MISSING")
        if int(lock.get("thread_id") or 0) <= 0:
            failures.append("RUNTIME_EXECUTION_LOCK_THREAD_ID_MISSING")
        if not lock.get("heartbeat_at"):
            failures.append("RUNTIME_EXECUTION_LOCK_HEARTBEAT_MISSING")
        if lock.get("state") == "ACTIVE" and not bool(lock.get("owner_alive")):
            failures.append("RUNTIME_EXECUTION_LOCK_ACTIVE_OWNER_NOT_ALIVE")

    if dashboard_status and (
        int(dashboard_status.get("lock_count") or 0)
        != int(status.get("lock_count") or 0)
    ):
        failures.append("DASHBOARD_RUNTIME_EXECUTION_LOCK_COUNT_MISMATCH")
    if not dashboard_status:
        warnings.append("DASHBOARD_RUNTIME_EXECUTION_LOCK_STATUS_MISSING")

    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures or warnings),
        "lock_count": int(status.get("lock_count") or 0),
        "active_count": int(status.get("active_count") or 0),
        "stale_expired_count": int(status.get("stale_expired_count") or 0),
        "expired_owner_alive_count": int(
            status.get("expired_owner_alive_count") or 0
        ),
        "no_trading_side_effects": bool(
            status.get("no_trading_side_effects", True)
        ),
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
            "# Runtime Execution Lock Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- lock_count: `{verdict.get('lock_count')}`",
            f"- active_count: `{verdict.get('active_count')}`",
            f"- stale_expired_count: `{verdict.get('stale_expired_count')}`",
            (
                "- expired_owner_alive_count: "
                f"`{verdict.get('expired_owner_alive_count')}`"
            ),
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "This check is read-only and does not clear or acquire locks.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return (
        "runtime execution lock: "
        f"{verdict.get('status')} locks={verdict.get('lock_count')} "
        f"active={verdict.get('active_count')} "
        f"stale={verdict.get('stale_expired_count')}"
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    data = payload.get("data")
    if isinstance(data, Mapping):
        return dict(data)
    return dict(payload)


if __name__ == "__main__":
    raise SystemExit(main())
