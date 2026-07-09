from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check PR-6 market_data append-only dry-run routing readiness."
    )
    parser.add_argument(
        "--core-url",
        default=os.environ.get("TRADING_CORE_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("TRADING_CORE_TOKEN") or os.environ.get("GATEWAY_CORE_TOKEN", ""),
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "market_data_append_only_routing"),
    )
    args = parser.parse_args()

    report = run_routing_report(
        core_url=args.core_url,
        token=args.token,
        limit=args.limit,
        timeout_sec=args.timeout_sec,
        out_dir=Path(args.out_dir),
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_routing_report(
    *,
    core_url: str,
    token: str,
    limit: int,
    timeout_sec: float,
    out_dir: Path,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    status_payload = fetch_json(
        f"{base_url}/api/operator/market-data-append-only-routing/status",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    decisions_payload = fetch_json(
        f"{base_url}/api/operator/market-data-append-only-routing/decisions?"
        f"{urllib.parse.urlencode({'limit': str(limit)})}",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    reconcile_payload = fetch_json(
        f"{base_url}/api/operator/market-data-projection-reconcile/latest",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    outbox_payload = fetch_json(
        f"{base_url}/api/operator/projection-outbox/status",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    dashboard_params = {
        "sections": "market_data,projection_outbox,pipeline_summary,gateway,errors",
        "detail": "summary",
        "limit": "20",
        "fast": "true",
        "timeout_budget_ms": "5000",
    }
    dashboard_payload = fetch_json(
        f"{base_url}/api/dashboard/snapshot?{urllib.parse.urlencode(dashboard_params)}",
        token=token,
        method="GET",
        timeout_sec=timeout_sec,
    )
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report = {
        "generated_at": generated_at,
        "core_url": base_url,
        "routing_status": status_payload,
        "routing_decisions": decisions_payload,
        "latest_reconcile": reconcile_payload,
        "projection_outbox": outbox_payload,
        "dashboard_snapshot": dashboard_payload,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in (
        "routing_status",
        "routing_decisions",
        "latest_reconcile",
        "projection_outbox",
        "dashboard_snapshot",
    ):
        payload = report.get(key) or {}
        if not payload.get("ok"):
            failures.append(f"{key.upper()}_API_ERROR")

    status = _data(report, "routing_status")
    reconcile = _data(report, "latest_reconcile")
    outbox = _data(report, "projection_outbox")
    dashboard = _data(report, "dashboard_snapshot")
    latest_run = reconcile.get("latest_run") if isinstance(reconcile, dict) else None
    dashboard_routing = (
        dashboard.get("pipeline_summary", {}).get("market_data_append_only_routing", {})
        if isinstance(dashboard, dict)
        else {}
    )
    effective_skip_count = int(status.get("effective_skip_inline_count") or 0)
    would_skip_count = int(status.get("would_skip_inline_count") or 0)
    total_decision_count = int(status.get("total_decision_count") or 0)
    checked_event_count = (
        int(latest_run.get("checked_event_count") or 0)
        if isinstance(latest_run, dict)
        else 0
    )
    append_only_ready = bool(status.get("append_only_ready"))

    if effective_skip_count > 0:
        failures.append("EFFECTIVE_SKIP_INLINE_OCCURRED_IN_PR6")
    if bool(status.get("cutover_enabled")) and effective_skip_count > 0:
        failures.append("CUTOVER_ENABLED_WITH_EFFECTIVE_SKIP")
    if isinstance(latest_run, dict) and latest_run.get("status") == "FAIL":
        failures.append("LATEST_RECONCILE_FAIL")
    if not append_only_ready and would_skip_count > 0:
        failures.append("WOULD_SKIP_WITHOUT_APPEND_ONLY_READY")
    if bool(status.get("dry_run_enabled")) and total_decision_count <= 0:
        failures.append("ROUTING_DECISION_MISSING")
    if int(dashboard_routing.get("effective_skip_inline_count") or 0) != effective_skip_count:
        failures.append("DASHBOARD_ROUTING_STATUS_MISMATCH")

    if not bool(status.get("dry_run_enabled")):
        warnings.append("DRY_RUN_DISABLED")
    if not isinstance(latest_run, dict):
        warnings.append("LATEST_RECONCILE_MISSING")
    elif latest_run.get("status") not in {"PASS", "WARN"}:
        warnings.append("LATEST_RECONCILE_NOT_READY")
    if int(outbox.get("pending_count") or 0) > 0:
        warnings.append("PROJECTION_OUTBOX_PENDING")
    if int(outbox.get("error_count") or 0) > 0 or int(outbox.get("dead_letter_count") or 0) > 0:
        warnings.append("PROJECTION_OUTBOX_ERROR")
    if would_skip_count == 0 and checked_event_count > 0:
        warnings.append("NO_WOULD_SKIP_DECISIONS_WITH_CHECKED_EVENTS")

    verdict = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
    }


def fetch_json(
    url: str,
    *,
    token: str,
    method: str,
    timeout_sec: float,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if token:
        headers["X-Local-Token"] = token
        headers["X-Core-Token"] = token
    request = urllib.request.Request(url, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read().decode("utf-8")
            return {
                "ok": True,
                "status_code": int(response.status),
                "url": url,
                "data": json.loads(body) if body.strip() else {},
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "status_code": int(exc.code),
            "url": url,
            "error": body or str(exc),
            "data": _json_or_empty(body),
        }
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {
            "ok": False,
            "status_code": None,
            "url": url,
            "error": str(exc),
            "data": {},
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
    status = _data(report, "routing_status")
    verdict = report.get("verdict", {})
    lines = [
        "# Market Data Append-Only Routing Check",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- dry_run_enabled: `{status.get('dry_run_enabled')}`",
        f"- cutover_enabled: `{status.get('cutover_enabled')}`",
        f"- append_only_ready: `{status.get('append_only_ready')}`",
        f"- total_decision_count: `{status.get('total_decision_count')}`",
        f"- would_skip_inline_count: `{status.get('would_skip_inline_count')}`",
        f"- effective_skip_inline_count: `{status.get('effective_skip_inline_count')}`",
        f"- failures: `{verdict.get('failures', [])}`",
        f"- warnings: `{verdict.get('warnings', [])}`",
        "",
        "## Safety",
        "",
        "- PR-6 dry-run only; inline projection remains enabled.",
        "- effective_skip_inline must remain 0.",
        "- LIVE_SIM/LIVE_REAL/order behavior is not changed by this check.",
    ]
    return "\n".join(lines) + "\n"


def render_console_summary(report: dict[str, Any]) -> str:
    status = _data(report, "routing_status")
    verdict = report.get("verdict", {})
    return (
        "market_data append-only routing: "
        f"{verdict.get('status')} "
        f"dry_run={status.get('dry_run_enabled')} "
        f"would_skip={status.get('would_skip_inline_count')} "
        f"effective_skip={status.get('effective_skip_inline_count')}"
    )


def _data(report: dict[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _json_or_empty(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
