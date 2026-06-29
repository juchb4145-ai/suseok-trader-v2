from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


STAGES = (
    "Core",
    "Gateway",
    "MarketData",
    "Theme",
    "Candidate",
    "Strategy",
    "Risk",
    "EntryTiming",
    "LiveSim",
    "OrderSafety",
)
STATUS_RANK = {"UNKNOWN": 0, "PASS": 1, "WARN": 2, "BLOCK": 3}


@dataclass(frozen=True, kw_only=True)
class DiagnosticEndpoint:
    key: str
    stage: str
    path: str


ENDPOINTS: tuple[DiagnosticEndpoint, ...] = (
    DiagnosticEndpoint(key="health", stage="Core", path="/health"),
    DiagnosticEndpoint(key="api_status", stage="Core", path="/api/status"),
    DiagnosticEndpoint(key="gateway_auth_probe", stage="Gateway", path="/api/gateway/auth/probe"),
    DiagnosticEndpoint(key="gateway_status", stage="Gateway", path="/api/gateway/status"),
    DiagnosticEndpoint(
        key="gateway_events_recent",
        stage="Gateway",
        path="/api/gateway/events/recent?limit=50",
    ),
    DiagnosticEndpoint(
        key="gateway_commands_status",
        stage="Gateway",
        path="/api/gateway/commands/status",
    ),
    DiagnosticEndpoint(
        key="market_data_status",
        stage="MarketData",
        path="/api/market-data/status",
    ),
    DiagnosticEndpoint(
        key="market_data_ticks_latest",
        stage="MarketData",
        path="/api/market-data/ticks/latest",
    ),
    DiagnosticEndpoint(
        key="market_data_projection_errors",
        stage="MarketData",
        path="/api/market-data/projection-errors",
    ),
    DiagnosticEndpoint(
        key="market_data_conditions_recent",
        stage="MarketData",
        path="/api/market-data/conditions/recent",
    ),
    DiagnosticEndpoint(key="themes_status", stage="Theme", path="/api/themes/status"),
    DiagnosticEndpoint(key="themes", stage="Theme", path="/api/themes"),
    DiagnosticEndpoint(
        key="themes_snapshots_latest",
        stage="Theme",
        path="/api/themes/snapshots/latest",
    ),
    DiagnosticEndpoint(
        key="themes_projection_errors",
        stage="Theme",
        path="/api/themes/projection-errors",
    ),
    DiagnosticEndpoint(key="candidates_status", stage="Candidate", path="/api/candidates/status"),
    DiagnosticEndpoint(key="candidates", stage="Candidate", path="/api/candidates"),
    DiagnosticEndpoint(
        key="candidates_projection_errors",
        stage="Candidate",
        path="/api/candidates/projection-errors",
    ),
    DiagnosticEndpoint(key="strategy_status", stage="Strategy", path="/api/strategy/status"),
    DiagnosticEndpoint(key="strategy_runs", stage="Strategy", path="/api/strategy/runs"),
    DiagnosticEndpoint(key="strategy_errors", stage="Strategy", path="/api/strategy/errors"),
    DiagnosticEndpoint(key="risk_status", stage="Risk", path="/api/risk/status"),
    DiagnosticEndpoint(key="risk_runs", stage="Risk", path="/api/risk/runs"),
    DiagnosticEndpoint(key="risk_errors", stage="Risk", path="/api/risk/errors"),
    DiagnosticEndpoint(
        key="entry_timing_status",
        stage="EntryTiming",
        path="/api/entry-timing/status",
    ),
    DiagnosticEndpoint(
        key="entry_timing_plans_latest",
        stage="EntryTiming",
        path="/api/entry-timing/plans/latest",
    ),
    DiagnosticEndpoint(
        key="entry_timing_errors",
        stage="EntryTiming",
        path="/api/entry-timing/errors",
    ),
    DiagnosticEndpoint(key="live_sim_status", stage="LiveSim", path="/api/live-sim/status"),
    DiagnosticEndpoint(
        key="live_sim_operator_status",
        stage="LiveSim",
        path="/api/live-sim/operator/status",
    ),
    DiagnosticEndpoint(
        key="live_sim_operator_run_latest",
        stage="LiveSim",
        path="/api/live-sim/operator/runs/latest",
    ),
    DiagnosticEndpoint(
        key="live_sim_rejections",
        stage="LiveSim",
        path="/api/live-sim/rejections",
    ),
    DiagnosticEndpoint(key="live_sim_errors", stage="LiveSim", path="/api/live-sim/errors"),
    DiagnosticEndpoint(
        key="live_sim_reconcile_latest",
        stage="LiveSim",
        path="/api/live-sim/reconcile/latest",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect and classify market-open runtime RCA from Core API."
    )
    parser.add_argument("--core-url", default=os.environ.get("TRADING_CORE_URL", "http://127.0.0.1:8000"))
    parser.add_argument(
        "--token",
        default=os.environ.get("TRADING_CORE_TOKEN") or os.environ.get("GATEWAY_CORE_TOKEN", ""),
    )
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--report-root", default=None)
    args = parser.parse_args()

    endpoint_results = collect_endpoints(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
    )
    summary = classify_market_open_rca(
        endpoint_results,
        core_url=args.core_url,
        trade_date=args.trade_date,
    )
    report_root = Path(args.out_dir or args.report_root or ROOT_DIR / "reports" / "market_open_rca")
    paths = write_rca_report(summary, report_root=report_root)
    summary["report_paths"] = {key: str(value) for key, value in paths.items()}
    paths["summary_json"].write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(render_console_summary(summary))
    return 0 if summary["overall_status"] in {"PASS", "WARN"} else 2


def collect_endpoints(
    *,
    core_url: str,
    token: str | None,
    timeout_sec: float = 5.0,
) -> dict[str, dict[str, Any]]:
    base_url = core_url.rstrip("/")
    results: dict[str, dict[str, Any]] = {}
    for endpoint in ENDPOINTS:
        url = _join_url(base_url, endpoint.path)
        results[endpoint.key] = fetch_json(
            url,
            token=token,
            timeout_sec=timeout_sec,
            stage=endpoint.stage,
            endpoint=endpoint.path,
        )
    return results


def fetch_json(
    url: str,
    *,
    token: str | None,
    timeout_sec: float,
    stage: str,
    endpoint: str,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if token:
        headers["X-Core-Token"] = token
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read().decode("utf-8")
            payload = json.loads(body) if body.strip() else {}
            return {
                "ok": True,
                "status_code": int(response.status),
                "url": url,
                "endpoint": endpoint,
                "stage": stage,
                "data": payload,
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "status_code": int(exc.code),
            "url": url,
            "endpoint": endpoint,
            "stage": stage,
            "error": body or str(exc),
            "data": _json_or_empty(body),
        }
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {
            "ok": False,
            "status_code": None,
            "url": url,
            "endpoint": endpoint,
            "stage": stage,
            "error": str(exc),
            "data": {},
        }
    except json.JSONDecodeError as exc:
        return {
            "ok": False,
            "status_code": None,
            "url": url,
            "endpoint": endpoint,
            "stage": stage,
            "error": f"invalid JSON response: {exc}",
            "data": {},
        }


def classify_market_open_rca(
    endpoint_results: Mapping[str, Mapping[str, Any]],
    *,
    core_url: str = "",
    trade_date: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    stages = _initial_stages()
    _classify_endpoint_errors(stages, endpoint_results)
    _classify_core(stages, endpoint_results)
    _classify_gateway(stages, endpoint_results)
    _classify_market_data(stages, endpoint_results)
    _classify_theme(stages, endpoint_results)
    _classify_candidate(stages, endpoint_results)
    _classify_strategy(stages, endpoint_results)
    _classify_risk(stages, endpoint_results)
    _classify_entry_timing(stages, endpoint_results)
    _classify_live_sim(stages, endpoint_results)
    _classify_order_safety(stages, endpoint_results)
    stage_list = [_finalize_stage(stages[stage]) for stage in STAGES]
    return {
        "trade_date": trade_date,
        "core_url": core_url,
        "generated_at": generated_at or _now(),
        "overall_status": _overall_status(stage_list),
        "stages": stage_list,
        "endpoint_results": dict(endpoint_results),
        "reason_codes": _dedupe(
            [
                reason
                for stage in stage_list
                for reason in stage.get("reason_codes", [])
            ]
        ),
        "read_only": True,
        "queue_commands": False,
        "live_real_allowed": False,
    }


def write_rca_report(
    summary: Mapping[str, Any],
    *,
    report_root: Path,
) -> dict[str, Path]:
    trade_date = str(summary.get("trade_date") or date.today().isoformat())
    report_dir = report_root / trade_date
    report_dir.mkdir(parents=True, exist_ok=True)
    summary_json = report_dir / "summary.json"
    summary_md = report_dir / "summary.md"
    summary_json.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_md.write_text(render_markdown_summary(summary), encoding="utf-8")
    return {"summary_json": summary_json, "summary_md": summary_md}


def render_console_summary(summary: Mapping[str, Any]) -> str:
    lines = [
        f"Market Open RCA: {summary.get('overall_status')} ({summary.get('trade_date')})",
        f"Core URL: {summary.get('core_url')}",
        "",
    ]
    for stage in summary.get("stages", []):
        reasons = ",".join(stage.get("reason_codes", [])) or "-"
        lines.append(
            f"{stage.get('stage'):12} {stage.get('status'):7} "
            f"{reasons:36} {stage.get('summary') or ''}"
        )
    report_paths = summary.get("report_paths") or {}
    if report_paths:
        lines.extend(
            [
                "",
                f"summary.json: {report_paths.get('summary_json')}",
                f"summary.md: {report_paths.get('summary_md')}",
            ]
        )
    return "\n".join(lines)


def render_markdown_summary(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Market Open Runtime RCA",
        "",
        f"- trade_date: `{summary.get('trade_date')}`",
        f"- overall_status: `{summary.get('overall_status')}`",
        f"- generated_at: `{summary.get('generated_at')}`",
        f"- core_url: `{summary.get('core_url')}`",
        "- mode: read-only, queue_commands=false, LIVE_REAL disallowed",
        "",
        "| Stage | Status | Reason codes | Summary |",
        "| --- | --- | --- | --- |",
    ]
    for stage in summary.get("stages", []):
        reasons = ", ".join(stage.get("reason_codes", [])) or "-"
        lines.append(
            "| {stage} | {status} | {reasons} | {summary_text} |".format(
                stage=stage.get("stage"),
                status=stage.get("status"),
                reasons=reasons,
                summary_text=str(stage.get("summary") or "").replace("|", "\\|"),
            )
        )
    error_detail_lines = _render_error_detail_lines(summary)
    if error_detail_lines:
        lines.extend(["", "## Error Details", "", *error_detail_lines])
    lines.extend(
        [
            "",
            "## Operator Notes",
            "",
            "- `PASS`는 해당 관찰 단계가 읽히거나 기대된 안전 차단 상태라는 뜻입니다.",
            "- `WARN`은 다음 단계가 비어 있거나 아직 run_once/rebuild가 필요할 수 있다는 뜻입니다.",
            "- `BLOCK`은 인증, Core/Gateway, projection error처럼 먼저 해소해야 하는 지점입니다.",
            "- `LIVE_SIM_DISABLED_EXPECTED`, `LIVE_SIM_KILL_SWITCH_ON_EXPECTED`, "
            "`ORDER_COMMAND_ZERO_EXPECTED`는 observe-only 운영에서 정상 안전 상태입니다.",
            "",
        ]
    )
    return "\n".join(lines)


def _render_error_detail_lines(summary: Mapping[str, Any]) -> list[str]:
    rows: list[tuple[str, str, str, str, str]] = []
    for stage in summary.get("stages", []):
        if not isinstance(stage, Mapping):
            continue
        stage_name = str(stage.get("stage") or "")
        checks = stage.get("checks") if isinstance(stage.get("checks"), list) else []
        for check in checks:
            if not isinstance(check, Mapping):
                continue
            details = check.get("details")
            if not isinstance(details, Mapping):
                continue
            errors = details.get("errors")
            if not isinstance(errors, list):
                continue
            for error in errors[:5]:
                if not isinstance(error, Mapping):
                    continue
                rows.append(
                    (
                        stage_name,
                        str(error.get("event_id") or "-"),
                        str(error.get("code") or "-"),
                        str(error.get("error_message") or error.get("error") or "-"),
                        _json_excerpt(error.get("payload")),
                    )
                )
    if not rows:
        return []
    lines = [
        "| Stage | event_id | code | error | payload_excerpt |",
        "| --- | --- | --- | --- | --- |",
    ]
    for stage_name, event_id, code, error, payload in rows[:20]:
        lines.append(
            "| {stage} | {event_id} | {code} | {error} | {payload} |".format(
                stage=_md_cell(stage_name),
                event_id=_md_cell(event_id),
                code=_md_cell(code),
                error=_md_cell(error),
                payload=_md_cell(payload),
            )
        )
    return lines


def _initial_stages() -> dict[str, dict[str, Any]]:
    return {
        stage: {
            "stage": stage,
            "status": "UNKNOWN",
            "reason_codes": [],
            "summary": "",
            "checks": [],
            "details": {},
        }
        for stage in STAGES
    }


def _mark(
    stages: dict[str, dict[str, Any]],
    stage: str,
    status: str,
    reason_codes: str | Sequence[str] | None = None,
    summary: str = "",
    details: Mapping[str, Any] | None = None,
) -> None:
    target = stages[stage]
    if STATUS_RANK[status] >= STATUS_RANK[target["status"]]:
        target["status"] = status
        if summary:
            target["summary"] = summary
    if reason_codes:
        values = [reason_codes] if isinstance(reason_codes, str) else list(reason_codes)
        target["reason_codes"] = _dedupe([*target["reason_codes"], *values])
    if details:
        target["checks"].append({"status": status, "summary": summary, "details": dict(details)})


def _classify_endpoint_errors(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    for key, result in endpoint_results.items():
        if result.get("ok"):
            continue
        stage = str(result.get("stage") or _endpoint_stage(key))
        status_code = result.get("status_code")
        endpoint = str(result.get("endpoint") or key)
        if status_code in {401, 403}:
            _mark(
                stages,
                "Gateway",
                "BLOCK",
                "GATEWAY_AUTH_FAILED",
                f"Auth failed at {endpoint}: HTTP {status_code}",
                {"endpoint": endpoint, "status_code": status_code},
            )
        elif status_code is None and stage == "Core":
            _mark(
                stages,
                "Core",
                "BLOCK",
                "CORE_DOWN",
                f"Core endpoint failed: {endpoint}",
                {"endpoint": endpoint, "error": result.get("error")},
            )
        elif isinstance(status_code, int) and status_code >= 500:
            reason = (
                "CORE_STATUS_ERROR"
                if stage == "Core"
                else f"{_code_stage(stage)}_STATUS_ERROR"
            )
            _mark(
                stages,
                stage,
                "BLOCK",
                reason,
                f"{endpoint} returned HTTP {status_code}",
                {"endpoint": endpoint, "status_code": status_code},
            )
        else:
            _mark(
                stages,
                stage,
                "UNKNOWN",
                None,
                f"{endpoint} could not be read",
                {"endpoint": endpoint, "status_code": status_code, "error": result.get("error")},
            )


def _classify_core(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    health = _payload(endpoint_results, "health")
    api_status = _payload(endpoint_results, "api_status")
    if health.get("status") == "ok" and api_status.get("status") == "ok":
        _mark(stages, "Core", "PASS", None, "Core health and /api/status are ok.")
    if api_status.get("live_real_allowed") is True:
        _mark(
            stages,
            "Core",
            "BLOCK",
            "CORE_STATUS_ERROR",
            "LIVE_REAL appears allowed in /api/status.",
            api_status,
        )


def _classify_gateway(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    status = _payload(endpoint_results, "gateway_status")
    if not status:
        return
    events = _list_from_payload(_payload(endpoint_results, "gateway_events_recent"), "events")
    heartbeat_at = status.get("last_heartbeat_at")
    if not heartbeat_at:
        _mark(
            stages,
            "Gateway",
            "BLOCK",
            "GATEWAY_HEARTBEAT_MISSING",
            "Gateway heartbeat is missing.",
            status,
        )
    else:
        age = _age_seconds(heartbeat_at)
        if age is not None and age > 120:
            _mark(
                stages,
                "Gateway",
                "WARN",
                "GATEWAY_HEARTBEAT_MISSING",
                f"Gateway heartbeat is stale: {int(age)} seconds old.",
                {"last_heartbeat_at": heartbeat_at, "heartbeat_age_sec": age},
            )
        else:
            _mark(stages, "Gateway", "PASS", None, "Gateway heartbeat exists.")
    if not events:
        _mark(
            stages,
            "Gateway",
            "WARN",
            "GATEWAY_HEARTBEAT_MISSING",
            "No recent gateway events were returned.",
        )
    _classify_gateway_recent_events(stages, events)
    command_counts = _dict_or_empty(
        _payload(endpoint_results, "gateway_commands_status").get("counts")
    )
    failed = int(command_counts.get("FAILED") or 0) + int(command_counts.get("REJECTED") or 0)
    if failed:
        _mark(
            stages,
            "Gateway",
            "WARN",
            "CORE_STATUS_ERROR",
            f"Gateway command failures exist: {failed}",
            {"counts": command_counts},
        )


def _classify_gateway_recent_events(
    stages: dict[str, dict[str, Any]],
    events: Sequence[Any],
) -> None:
    event_items = [_dict_or_empty(event) for event in events]
    latest_heartbeat = _latest_event_payload(event_items, "heartbeat")
    error_messages = [
        str(_dict_or_empty(event.get("payload")).get("message") or "")
        for event in event_items
        if str(event.get("event_type") or "").lower() == "gateway_error"
    ]
    login_failed = any(
        str(event.get("event_type") or "").lower() == "login_status"
        and _dict_or_empty(event.get("payload")).get("logged_in") is False
        for event in event_items
    ) or any("KIWOOM_LOGIN" in message for message in error_messages)
    login_block_reason_codes = [
        str(code)
        for code in list(latest_heartbeat.get("login_block_reason_codes") or [])
        if str(code or "").strip()
    ]
    comm_connect_state = str(latest_heartbeat.get("comm_connect_state") or "").upper()
    if login_block_reason_codes or comm_connect_state.startswith("EVENT_TIMEOUT"):
        _mark(
            stages,
            "Gateway",
            "BLOCK",
            login_block_reason_codes
            or [
                "ON_EVENT_CONNECT_TIMEOUT",
                "ACTIVE_X_LOGIN_BLOCKED",
            ],
            (
                "Kiwoom CommConnect/OnEventConnect did not complete; "
                "realtime registration was not attempted."
            ),
            {"latest_heartbeat": latest_heartbeat, "gateway_errors": error_messages[:5]},
        )
    if (
        not login_failed
        and latest_heartbeat.get("login_requested") is True
        and latest_heartbeat.get("login_in_progress") is False
        and latest_heartbeat.get("login_result_code") not in {None, "", 0}
    ):
        login_failed = True
    if login_failed:
        _mark(
            stages,
            "Gateway",
            "BLOCK",
            "KIWOOM_LOGIN_FAILED",
            "Kiwoom login failure was reported by Gateway events.",
            {"latest_heartbeat": latest_heartbeat, "gateway_errors": error_messages[:5]},
        )

    condition_load_failed = any(
        str(event.get("event_type") or "").lower() == "condition_load_result"
        and _dict_or_empty(event.get("payload")).get("success") is False
        for event in event_items
    ) or str(latest_heartbeat.get("condition_load_state") or "").upper() == "FAILED"
    condition_load_failed = condition_load_failed or any(
        "CONDITION_LOAD_FAILED" in message for message in error_messages
    )
    if condition_load_failed:
        _mark(
            stages,
            "Gateway",
            "BLOCK",
            "CONDITION_LOAD_FAILED",
            "Condition load failed in recent Gateway events.",
            {"latest_heartbeat": latest_heartbeat, "gateway_errors": error_messages[:5]},
        )
    elif str(latest_heartbeat.get("condition_load_state") or "").upper() == "CALLBACK_TIMEOUT":
        _mark(
            stages,
            "Gateway",
            "BLOCK",
            [
                "CONDITION_VER_CALLBACK_TIMEOUT",
                "CONDITION_LOAD_TIMEOUT",
                "ACTIVE_X_CALLBACK_SUSPECTED",
                "POSSIBLE_THREADING_ISSUE",
            ],
            (
                "GetConditionLoad was requested, but OnReceiveConditionVer did not "
                "arrive before timeout."
            ),
            {"latest_heartbeat": latest_heartbeat, "gateway_errors": error_messages[:5]},
        )
    else:
        condition_load_success = any(
            str(event.get("event_type") or "").lower()
            in {"condition_loaded", "condition_load_result"}
            and _dict_or_empty(event.get("payload")).get("success", True) is not False
            for event in event_items
        )
        condition_sent = any(
            str(event.get("event_type") or "").lower() == "condition_event"
            for event in event_items
        ) or any(
            "configured condition send requested" in message.lower()
            for message in [
                str(_dict_or_empty(event.get("payload")).get("message") or "")
                for event in event_items
                if str(event.get("event_type") or "").lower() == "gateway_log"
            ]
        )
        if condition_load_success and not condition_sent:
            _mark(
                stages,
                "Gateway",
                "WARN",
                "CONDITION_NOT_SENT",
                "Conditions loaded, but no configured condition send or hit is visible.",
            )

    has_enter_condition = any(
        str(event.get("event_type") or "").lower() == "condition_event"
        and str(_dict_or_empty(event.get("payload")).get("action") or "").upper() == "ENTER"
        for event in event_items
    )
    has_price_tick = any(
        str(event.get("event_type") or "").lower() == "price_tick" for event in event_items
    )
    registered_count = _optional_int(latest_heartbeat.get("registered_realtime_code_count"))
    if has_enter_condition and not has_price_tick and registered_count == 0:
        _mark(
            stages,
            "Gateway",
            "WARN",
            "REALTIME_NOT_REGISTERED",
            "Condition ENTER exists, but no realtime registration/tick is visible.",
            {"latest_heartbeat": latest_heartbeat},
        )
    callback_count = _optional_int(latest_heartbeat.get("realtime_callback_count"))
    latest_callback_at = str(latest_heartbeat.get("latest_realtime_callback_at") or "").strip()
    recover_count = _optional_int(latest_heartbeat.get("realtime_recover_count")) or 0
    health = str(latest_heartbeat.get("realtime_subscription_health") or "").upper()
    if health == "PARSE_ERROR":
        _mark(
            stages,
            "Gateway",
            "BLOCK",
            "REALTIME_PARSE_ERROR",
            "Realtime callbacks arrived, but price tick parsing failed.",
            {"latest_heartbeat": latest_heartbeat},
        )
    if (
        health == "CALLBACK_TIMEOUT"
        or registered_count
        and registered_count > 0
        and not has_price_tick
        and not callback_count
    ):
        _mark(
            stages,
            "Gateway",
            "BLOCK" if health == "CALLBACK_TIMEOUT" or recover_count > 0 else "WARN",
            ["REALTIME_CALLBACK_MISSING", "ACTIVE_X_CALLBACK_SUSPECTED"],
            (
                "Realtime registration exists, but Kiwoom OnReceiveRealData callbacks "
                "are not visible."
            ),
            {
                "latest_heartbeat": latest_heartbeat,
                "latest_realtime_callback_at": latest_callback_at,
                "realtime_recover_count": recover_count,
            },
        )


def _classify_market_data(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    if _endpoint_unavailable(endpoint_results, "market_data_status") and _endpoint_unavailable(
        endpoint_results, "market_data_ticks_latest"
    ):
        return
    status = _payload(endpoint_results, "market_data_status")
    ticks = _list_from_payload(_payload(endpoint_results, "market_data_ticks_latest"), "ticks")
    errors = _list_from_payload(
        _payload(endpoint_results, "market_data_projection_errors"),
        "errors",
    )
    conditions_payload = _payload(endpoint_results, "market_data_conditions_recent")
    conditions = _list_from_payload_any(conditions_payload, ("signals", "conditions"))
    projection_error_count = int(status.get("projection_error_count") or len(errors))
    recent_projection_error_count = int(status.get("recent_projection_error_count") or 0)
    if recent_projection_error_count:
        _mark(
            stages,
            "MarketData",
            "BLOCK",
            "MARKET_PROJECTION_ERROR",
            (
                "Recent market projection errors exist: "
                f"{recent_projection_error_count}"
            ),
            {"errors": errors[:5], "status": status},
        )
    elif projection_error_count or errors:
        _mark(
            stages,
            "MarketData",
            "WARN",
            None,
            f"Historical market projection errors exist: {projection_error_count or len(errors)}",
            {"errors": errors[:5], "status": status},
        )
    if not ticks:
        _mark(stages, "MarketData", "BLOCK", "TICK_MISSING", "No latest ticks exist.")
    else:
        stale_sec = int(status.get("tick_stale_sec") or 30)
        stale_ticks = [
            tick
            for tick in ticks
            if (_age_seconds(tick.get("event_ts") or tick.get("updated_at")) or 0) > stale_sec
        ]
        if len(stale_ticks) == len(ticks):
            _mark(
                stages,
                "MarketData",
                "BLOCK",
                "TICK_STALE",
                f"All latest ticks are stale by threshold {stale_sec}s.",
                {"stale_count": len(stale_ticks), "tick_count": len(ticks)},
            )
        else:
            _mark(stages, "MarketData", "PASS", None, f"latest_tick_count={len(ticks)}")
    if not conditions:
        _mark(
            stages,
            "MarketData",
            "WARN",
            "NO_CONDITION_HIT",
            "No recent condition events exist.",
        )


def _classify_theme(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    if _endpoint_unavailable(endpoint_results, "themes_status"):
        return
    status = _payload(endpoint_results, "themes_status")
    themes = _list_from_payload(_payload(endpoint_results, "themes"), "themes")
    snapshots = _list_from_payload(
        _payload(endpoint_results, "themes_snapshots_latest"),
        "snapshots",
    )
    errors = _list_from_payload(_payload(endpoint_results, "themes_projection_errors"), "errors")
    state_counts = _counts_by_key(snapshots, "state")
    member_count = int(status.get("member_count") or 0)
    active_theme_count = int(status.get("active_theme_count") or len(themes))
    if member_count <= 0 or active_theme_count <= 0:
        _mark(
            stages,
            "Theme",
            "BLOCK",
            "THEME_MEMBERSHIP_EMPTY",
            "Theme membership is empty.",
            {"status": status, "theme_count": len(themes)},
        )
    elif not snapshots or int(status.get("latest_snapshot_count") or 0) <= 0:
        _mark(
            stages,
            "Theme",
            "WARN",
            "THEME_SNAPSHOT_NOT_BUILT",
            "No latest theme snapshot exists.",
            {"status": status},
        )
    else:
        _mark(
            stages,
            "Theme",
            "PASS",
            None,
            (
                f"theme_snapshots={len(snapshots)}, "
                f"LEADING={state_counts.get('LEADING', 0)}, "
                f"SPREADING={state_counts.get('SPREADING', 0)}"
            ),
            {"status": status, "state_counts": state_counts},
        )
    if errors:
        _mark(
            stages,
            "Theme",
            "BLOCK",
            "THEME_SNAPSHOT_NOT_BUILT",
            f"Theme projection errors exist: {len(errors)}",
            {"errors": errors[:5]},
        )


def _classify_candidate(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    if _endpoint_unavailable(endpoint_results, "candidates_status"):
        return
    status = _payload(endpoint_results, "candidates_status")
    candidates = _list_from_payload(_payload(endpoint_results, "candidates"), "candidates")
    errors = _list_from_payload(
        _payload(endpoint_results, "candidates_projection_errors"),
        "errors",
    )
    active_count = int(status.get("active_candidate_count") or len(candidates))
    state_counts = _dict_or_empty(status.get("state_counts"))
    data_wait_count = int(state_counts.get("DATA_WAIT") or 0)
    if errors:
        _mark(
            stages,
            "Candidate",
            "BLOCK",
            "CANDIDATE_REBUILD_NOT_RUN",
            f"Candidate projection errors exist: {len(errors)}",
            {"errors": errors[:5]},
        )
    elif data_wait_count:
        _mark(
            stages,
            "Candidate",
            "WARN",
            "CANDIDATE_DATA_WAIT",
            f"Candidates are waiting for data/context: {data_wait_count}",
            {"status": status},
        )
    elif active_count <= 0:
        reasons = ["CANDIDATE_EMPTY"]
        theme_status = _payload(endpoint_results, "themes_status")
        market_ticks = _list_from_payload(_payload(endpoint_results, "market_data_ticks_latest"), "ticks")
        if int(theme_status.get("member_count") or 0) <= 0:
            reasons.append("THEME_MEMBERSHIP_EMPTY")
        if not market_ticks:
            reasons.append("TICK_MISSING")
        _mark(
            stages,
            "Candidate",
            "WARN",
            reasons,
            "No active candidates exist.",
            {"status": status, "upstream_theme_status": theme_status},
        )
    else:
        _mark(stages, "Candidate", "PASS", None, f"active_candidate_count={active_count}")


def _classify_strategy(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    if _endpoint_unavailable(endpoint_results, "strategy_status") and _endpoint_unavailable(
        endpoint_results, "strategy_runs"
    ):
        return
    status = _payload(endpoint_results, "strategy_status")
    runs = _list_from_payload(_payload(endpoint_results, "strategy_runs"), "runs")
    errors = _list_from_payload(_payload(endpoint_results, "strategy_errors"), "errors")
    if errors:
        _mark(
            stages,
            "Strategy",
            "BLOCK",
            "STRATEGY_EVALUATE_NOT_RUN",
            f"Strategy errors exist: {len(errors)}",
            {"errors": errors[:5]},
        )
    elif not runs:
        _mark(
            stages,
            "Strategy",
            "WARN",
            "STRATEGY_EVALUATE_NOT_RUN",
            "No strategy evaluation run exists.",
        )
    elif int(status.get("latest_observation_count") or 0) <= 0:
        _mark(stages, "Strategy", "WARN", "STRATEGY_EMPTY", "Strategy ran but is empty.")
    else:
        _mark(
            stages,
            "Strategy",
            "PASS",
            None,
            f"latest_observation_count={status.get('latest_observation_count')}",
        )


def _classify_risk(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    if _endpoint_unavailable(endpoint_results, "risk_status") and _endpoint_unavailable(
        endpoint_results, "risk_runs"
    ):
        return
    status = _payload(endpoint_results, "risk_status")
    runs = _list_from_payload(_payload(endpoint_results, "risk_runs"), "runs")
    errors = _list_from_payload(_payload(endpoint_results, "risk_errors"), "errors")
    if errors:
        _mark(
            stages,
            "Risk",
            "BLOCK",
            "RISK_EVALUATE_NOT_RUN",
            f"Risk errors exist: {len(errors)}",
            {"errors": errors[:5]},
        )
    elif not runs:
        _mark(stages, "Risk", "WARN", "RISK_EVALUATE_NOT_RUN", "No risk run exists.")
    elif int(status.get("latest_observation_count") or 0) <= 0:
        _mark(stages, "Risk", "WARN", "RISK_EMPTY", "Risk ran but is empty.")
    else:
        _mark(
            stages,
            "Risk",
            "PASS",
            None,
            f"latest_observation_count={status.get('latest_observation_count')}",
        )


def _classify_entry_timing(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    if _endpoint_unavailable(endpoint_results, "entry_timing_status"):
        return
    status = _payload(endpoint_results, "entry_timing_status")
    plans = _list_from_payload(
        _payload(endpoint_results, "entry_timing_plans_latest"),
        "order_plan_drafts",
    )
    errors = _list_from_payload(_payload(endpoint_results, "entry_timing_errors"), "errors")
    plan_counts = _entry_timing_plan_counts(status, plans)
    count_summary = _entry_timing_count_summary(plan_counts)
    if errors:
        _mark(
            stages,
            "EntryTiming",
            "BLOCK",
            "ENTRY_TIMING_NO_INPUT",
            f"EntryTiming errors exist: {len(errors)}; {count_summary}",
            {"status": status, "plan_status_counts": plan_counts, "errors": errors[:5]},
        )
    elif int(status.get("evaluation_count") or 0) <= 0:
        _mark(
            stages,
            "EntryTiming",
            "WARN",
            "ENTRY_TIMING_NO_INPUT",
            f"EntryTiming has not evaluated inputs; {count_summary}",
            {"status": status, "plan_status_counts": plan_counts},
        )
    elif not plans or int(status.get("latest_plan_count") or 0) <= 0:
        reason = "ENTRY_TIMING_NO_INPUT" if plan_counts["DATA_WAIT"] > 0 else "ORDER_PLAN_EMPTY"
        _mark(
            stages,
            "EntryTiming",
            "WARN",
            reason,
            f"EntryTiming evaluated but no order plan draft exists; {count_summary}",
            {"status": status, "plan_status_counts": plan_counts},
        )
    elif plan_counts["PLAN_READY"] <= 0:
        reason = "ENTRY_TIMING_NO_INPUT" if plan_counts["DATA_WAIT"] > 0 else "ORDER_PLAN_EMPTY"
        _mark(
            stages,
            "EntryTiming",
            "WARN",
            reason,
            f"Order plan drafts exist, but none are PLAN_READY; {count_summary}",
            {"status": status, "plan_status_counts": plan_counts, "order_plan_drafts": plans[:5]},
        )
    else:
        _mark(
            stages,
            "EntryTiming",
            "PASS",
            None,
            (
                f"order_plan_drafts={len(plans)}; {count_summary}; "
                "observe_only=true; not_order_intent=true"
            ),
            {"status": status, "plan_status_counts": plan_counts},
        )


def _classify_live_sim(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    if _endpoint_unavailable(endpoint_results, "live_sim_status"):
        return
    status = _payload(endpoint_results, "live_sim_status")
    operator_status = _payload(endpoint_results, "live_sim_operator_status")
    rejections = _list_from_payload(_payload(endpoint_results, "live_sim_rejections"), "rejections")
    errors = _list_from_payload(_payload(endpoint_results, "live_sim_errors"), "errors")
    reasons: list[str] = []
    if status.get("enabled") is False:
        reasons.append("LIVE_SIM_DISABLED_EXPECTED")
    if status.get("kill_switch") is True:
        reasons.append("LIVE_SIM_KILL_SWITCH_ON_EXPECTED")
    if errors:
        _mark(
            stages,
            "LiveSim",
            "BLOCK",
            reasons,
            f"LIVE_SIM errors exist: {len(errors)}",
            {"errors": errors[:5]},
        )
    elif rejections:
        _mark(
            stages,
            "LiveSim",
            "WARN",
            reasons,
            f"LIVE_SIM rejections exist: {len(rejections)}",
            {"rejections": rejections[:5]},
        )
    else:
        _mark(
            stages,
            "LiveSim",
            "PASS",
            reasons,
            "LIVE_SIM read-only status collected; command queue remains disabled.",
            {"status": status, "operator_status": operator_status},
        )
    blocking_reasons = operator_status.get("blocking_reasons") or []
    if blocking_reasons:
        _mark(
            stages,
            "LiveSim",
            "WARN",
            reasons,
            "LIVE_SIM operator status has blocking reasons for order modes.",
            {"blocking_reasons": blocking_reasons},
        )


def _classify_order_safety(
    stages: dict[str, dict[str, Any]],
    endpoint_results: Mapping[str, Mapping[str, Any]],
) -> None:
    payload = _payload(endpoint_results, "gateway_commands_status")
    if not payload:
        _mark(
            stages,
            "OrderSafety",
            "UNKNOWN",
            None,
            "Gateway command status was not available.",
        )
        return
    command_type_counts = _dict_or_empty(payload.get("command_type_counts"))
    order_command_count = payload.get("order_command_count")
    if order_command_count is None:
        order_command_count = sum(
            int(command_type_counts.get(command_type) or 0)
            for command_type in (
                "send_order",
                "submit_order",
                "cancel_order",
                "modify_order",
                "enqueue_order",
                "order_intent",
                "gateway_order",
                "live_order",
            )
        )
    order_command_count = int(order_command_count or 0)
    if order_command_count:
        _mark(
            stages,
            "OrderSafety",
            "BLOCK",
            "ORDER_COMMAND_ZERO_EXPECTED",
            f"Order-like GatewayCommand count is {order_command_count}; expected zero in OBSERVE.",
            {"command_type_counts": command_type_counts},
        )
        return
    _mark(
        stages,
        "OrderSafety",
        "PASS",
        "ORDER_COMMAND_ZERO_EXPECTED",
        "No order-like GatewayCommand exists in command status.",
        {"command_type_counts": command_type_counts, "order_command_count": order_command_count},
    )


def _finalize_stage(stage: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "stage": stage["stage"],
        "status": stage["status"],
        "reason_codes": _dedupe(stage.get("reason_codes", [])),
        "summary": stage.get("summary") or "No signal was collected for this stage.",
        "checks": list(stage.get("checks", [])),
    }


def _overall_status(stages: Sequence[Mapping[str, Any]]) -> str:
    statuses = {str(stage.get("status")) for stage in stages}
    if "BLOCK" in statuses:
        return "BLOCK"
    if "WARN" in statuses:
        return "WARN"
    if "UNKNOWN" in statuses:
        return "UNKNOWN"
    return "PASS"


def _payload(
    endpoint_results: Mapping[str, Mapping[str, Any]],
    key: str,
) -> dict[str, Any]:
    result = endpoint_results.get(key) or {}
    data = result.get("data")
    return dict(data) if isinstance(data, Mapping) else {}


def _endpoint_unavailable(
    endpoint_results: Mapping[str, Mapping[str, Any]],
    key: str,
) -> bool:
    result = endpoint_results.get(key)
    if not result:
        return True
    return not bool(result.get("ok")) and not bool(result.get("data"))


def _list_from_payload(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    return list(value) if isinstance(value, list) else []


def _list_from_payload_any(payload: Mapping[str, Any], keys: Sequence[str]) -> list[Any]:
    for key in keys:
        values = _list_from_payload(payload, key)
        if values:
            return values
    return []


def _dict_or_empty(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _latest_event_payload(events: Sequence[Mapping[str, Any]], event_type: str) -> dict[str, Any]:
    normalized_type = event_type.strip().lower()
    for event in events:
        if str(event.get("event_type") or "").lower() == normalized_type:
            return _dict_or_empty(event.get("payload"))
    return {}


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _counts_by_key(items: Sequence[Any], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        value = str(item.get(key) or "UNKNOWN").upper()
        counts[value] = counts.get(value, 0) + 1
    return counts


def _entry_timing_plan_counts(
    status: Mapping[str, Any],
    plans: Sequence[Any],
) -> dict[str, int]:
    plan_status_counts = _counts_by_key(plans, "status")
    status_keys = {
        "PLAN_READY": "plan_ready_count",
        "WAIT_RETRY": "wait_retry_count",
        "DATA_WAIT": "data_wait_count",
        "NO_PLAN": "no_plan_count",
    }
    counts: dict[str, int] = {}
    for plan_status, status_key in status_keys.items():
        status_count = _optional_int(status.get(status_key))
        counts[plan_status] = (
            status_count if status_count is not None else plan_status_counts.get(plan_status, 0)
        )
    return counts


def _entry_timing_count_summary(counts: Mapping[str, int]) -> str:
    return ", ".join(
        f"{status}={int(counts.get(status, 0))}"
        for status in ("PLAN_READY", "WAIT_RETRY", "DATA_WAIT", "NO_PLAN")
    )


def _json_or_empty(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_excerpt(value: object, *, max_chars: int = 180) -> str:
    if value in (None, ""):
        return "-"
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        text = str(value)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def _md_cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _join_url(base_url: str, path: str) -> str:
    return urllib.parse.urljoin(f"{base_url}/", path.lstrip("/"))


def _now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _age_seconds(value: object) -> float | None:
    if not value:
        return None
    try:
        text = str(value)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max((datetime.now(tz=UTC) - parsed.astimezone(UTC)).total_seconds(), 0.0)


def _endpoint_stage(key: str) -> str:
    for endpoint in ENDPOINTS:
        if endpoint.key == key:
            return endpoint.stage
    return "Core"


def _code_stage(stage: str) -> str:
    if stage == "MarketData":
        return "MARKET_DATA"
    if stage == "EntryTiming":
        return "ENTRY_TIMING"
    if stage == "LiveSim":
        return "LIVE_SIM"
    return stage.upper()


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]


if __name__ == "__main__":
    raise SystemExit(main())
