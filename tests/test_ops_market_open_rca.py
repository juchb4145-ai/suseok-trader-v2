from __future__ import annotations

from datetime import UTC, datetime

from tools.ops_market_open_rca import classify_market_open_rca, write_rca_report


def test_rca_passes_with_read_only_order_safety_and_condition_signals() -> None:
    summary = classify_market_open_rca(
        _healthy_results(),
        core_url="http://127.0.0.1:8000",
        trade_date="2026-06-29",
        generated_at=_now(),
    )

    assert summary["overall_status"] == "PASS"
    assert summary["read_only"] is True
    assert summary["queue_commands"] is False
    assert _stage(summary, "MarketData")["reason_codes"] == []
    assert "LEADING=1" in _stage(summary, "Theme")["summary"]
    assert _stage(summary, "OrderSafety")["status"] == "PASS"
    assert "ORDER_COMMAND_ZERO_EXPECTED" in _stage(summary, "OrderSafety")["reason_codes"]


def test_rca_classifies_gateway_token_failure_as_block() -> None:
    results = _healthy_results()
    results["gateway_auth_probe"] = _fail(
        "Gateway",
        "/api/gateway/auth/probe",
        status_code=401,
        error="token required",
    )

    summary = classify_market_open_rca(results, generated_at=_now())
    gateway = _stage(summary, "Gateway")

    assert summary["overall_status"] == "BLOCK"
    assert gateway["status"] == "BLOCK"
    assert "GATEWAY_AUTH_FAILED" in gateway["reason_codes"]


def test_rca_keeps_missing_endpoint_unknown_not_false_block() -> None:
    results = _healthy_results()
    results["themes_status"] = _fail(
        "Theme",
        "/api/themes/status",
        status_code=404,
        error="not found",
    )

    summary = classify_market_open_rca(results, generated_at=_now())
    theme = _stage(summary, "Theme")

    assert summary["overall_status"] == "UNKNOWN"
    assert theme["status"] == "UNKNOWN"
    assert "THEME_MEMBERSHIP_EMPTY" not in theme["reason_codes"]


def test_rca_classifies_theme_snapshot_missing_separately_from_membership() -> None:
    results = _healthy_results()
    results["themes_status"]["data"]["latest_snapshot_count"] = 0
    results["themes_snapshots_latest"]["data"]["snapshots"] = []

    summary = classify_market_open_rca(results, generated_at=_now())
    theme = _stage(summary, "Theme")

    assert summary["overall_status"] == "WARN"
    assert theme["status"] == "WARN"
    assert "THEME_SNAPSHOT_NOT_BUILT" in theme["reason_codes"]
    assert "THEME_MEMBERSHIP_EMPTY" not in theme["reason_codes"]


def test_rca_classifies_candidate_data_wait() -> None:
    results = _healthy_results()
    results["candidates_status"]["data"]["state_counts"] = {"DATA_WAIT": 1}

    summary = classify_market_open_rca(results, generated_at=_now())
    candidate = _stage(summary, "Candidate")

    assert summary["overall_status"] == "WARN"
    assert candidate["status"] == "WARN"
    assert "CANDIDATE_DATA_WAIT" in candidate["reason_codes"]


def test_rca_candidate_empty_includes_upstream_tick_and_theme_reasons() -> None:
    results = _healthy_results()
    results["candidates_status"]["data"]["candidate_count"] = 0
    results["candidates_status"]["data"]["active_candidate_count"] = 0
    results["candidates"]["data"]["candidates"] = []
    results["themes_status"]["data"]["member_count"] = 0
    results["market_data_ticks_latest"]["data"]["ticks"] = []

    summary = classify_market_open_rca(results, generated_at=_now())
    candidate = _stage(summary, "Candidate")

    assert candidate["status"] == "WARN"
    assert "CANDIDATE_EMPTY" in candidate["reason_codes"]
    assert "THEME_MEMBERSHIP_EMPTY" in candidate["reason_codes"]
    assert "TICK_MISSING" in candidate["reason_codes"]


def test_rca_distinguishes_strategy_not_run_and_empty() -> None:
    not_run = _healthy_results()
    not_run["strategy_runs"]["data"]["runs"] = []

    not_run_summary = classify_market_open_rca(not_run, generated_at=_now())
    not_run_stage = _stage(not_run_summary, "Strategy")

    empty = _healthy_results()
    empty["strategy_status"]["data"]["latest_observation_count"] = 0

    empty_summary = classify_market_open_rca(empty, generated_at=_now())
    empty_stage = _stage(empty_summary, "Strategy")

    assert not_run_stage["status"] == "WARN"
    assert "STRATEGY_EVALUATE_NOT_RUN" in not_run_stage["reason_codes"]
    assert empty_stage["status"] == "WARN"
    assert "STRATEGY_EMPTY" in empty_stage["reason_codes"]


def test_rca_distinguishes_risk_not_run_and_empty() -> None:
    not_run = _healthy_results()
    not_run["risk_runs"]["data"]["runs"] = []

    not_run_summary = classify_market_open_rca(not_run, generated_at=_now())
    not_run_stage = _stage(not_run_summary, "Risk")

    empty = _healthy_results()
    empty["risk_status"]["data"]["latest_observation_count"] = 0

    empty_summary = classify_market_open_rca(empty, generated_at=_now())
    empty_stage = _stage(empty_summary, "Risk")

    assert not_run_stage["status"] == "WARN"
    assert "RISK_EVALUATE_NOT_RUN" in not_run_stage["reason_codes"]
    assert empty_stage["status"] == "WARN"
    assert "RISK_EMPTY" in empty_stage["reason_codes"]


def test_rca_entry_timing_summarizes_order_plan_status_counts() -> None:
    results = _healthy_results()
    results["entry_timing_status"]["data"].update(
        {
            "evaluation_count": 4,
            "latest_plan_count": 3,
            "plan_ready_count": 1,
            "wait_retry_count": 1,
            "data_wait_count": 1,
            "no_plan_count": 1,
        }
    )
    results["entry_timing_plans_latest"]["data"]["order_plan_drafts"] = [
        {"order_plan_id": "plan_ready", "status": "PLAN_READY"},
        {"order_plan_id": "wait_retry", "status": "WAIT_RETRY"},
        {"order_plan_id": "data_wait", "status": "DATA_WAIT"},
    ]

    summary = classify_market_open_rca(results, generated_at=_now())
    entry_timing = _stage(summary, "EntryTiming")

    assert entry_timing["status"] == "PASS"
    assert "PLAN_READY=1" in entry_timing["summary"]
    assert "WAIT_RETRY=1" in entry_timing["summary"]
    assert "DATA_WAIT=1" in entry_timing["summary"]
    assert "NO_PLAN=1" in entry_timing["summary"]
    assert "observe_only=true" in entry_timing["summary"]
    assert "not_order_intent=true" in entry_timing["summary"]
    assert _stage(summary, "OrderSafety")["status"] == "PASS"


def test_rca_entry_timing_no_input_and_data_wait_are_distinct() -> None:
    not_run = _healthy_results()
    not_run["entry_timing_status"]["data"].update(
        {
            "evaluation_count": 0,
            "latest_plan_count": 0,
            "plan_ready_count": 0,
            "wait_retry_count": 0,
            "data_wait_count": 0,
            "no_plan_count": 0,
        }
    )
    not_run["entry_timing_plans_latest"]["data"]["order_plan_drafts"] = []

    not_run_summary = classify_market_open_rca(not_run, generated_at=_now())
    not_run_stage = _stage(not_run_summary, "EntryTiming")

    data_wait = _healthy_results()
    data_wait["entry_timing_status"]["data"].update(
        {
            "evaluation_count": 1,
            "latest_plan_count": 1,
            "plan_ready_count": 0,
            "wait_retry_count": 0,
            "data_wait_count": 1,
            "no_plan_count": 0,
        }
    )
    data_wait["entry_timing_plans_latest"]["data"]["order_plan_drafts"] = [
        {"order_plan_id": "data_wait", "status": "DATA_WAIT"}
    ]

    data_wait_summary = classify_market_open_rca(data_wait, generated_at=_now())
    data_wait_stage = _stage(data_wait_summary, "EntryTiming")

    assert not_run_stage["status"] == "WARN"
    assert "ENTRY_TIMING_NO_INPUT" in not_run_stage["reason_codes"]
    assert "PLAN_READY=0" in not_run_stage["summary"]
    assert data_wait_stage["status"] == "WARN"
    assert "ENTRY_TIMING_NO_INPUT" in data_wait_stage["reason_codes"]
    assert "DATA_WAIT=1" in data_wait_stage["summary"]


def test_rca_classifies_empty_tick_response_as_tick_missing() -> None:
    results = _healthy_results()
    results["market_data_ticks_latest"] = _ok("MarketData", {"ticks": []})

    summary = classify_market_open_rca(results, generated_at=_now())
    market_data = _stage(summary, "MarketData")

    assert summary["overall_status"] == "BLOCK"
    assert market_data["status"] == "BLOCK"
    assert "TICK_MISSING" in market_data["reason_codes"]


def test_rca_classifies_stale_heartbeat_as_warn() -> None:
    results = _healthy_results()
    old = datetime(2026, 1, 1, tzinfo=UTC).isoformat()
    results["gateway_status"]["data"]["last_heartbeat_at"] = old
    results["gateway_events_recent"]["data"]["events"][0]["event_ts"] = old

    summary = classify_market_open_rca(results, generated_at=_now())
    gateway = _stage(summary, "Gateway")

    assert summary["overall_status"] == "WARN"
    assert gateway["status"] == "WARN"
    assert "GATEWAY_HEARTBEAT_MISSING" in gateway["reason_codes"]


def test_rca_classifies_projection_error_as_block(tmp_path) -> None:
    results = _healthy_results()
    results["market_data_status"]["data"]["projection_error_count"] = 1
    results["market_data_status"]["data"]["recent_projection_error_count"] = 1
    results["market_data_projection_errors"]["data"]["errors"] = [
        {
            "event_id": "evt_bad_tick",
            "code": "005930",
            "error_message": "invalid price",
            "payload": {"code": "005930", "price": 0},
        }
    ]

    summary = classify_market_open_rca(results, generated_at=_now())
    market_data = _stage(summary, "MarketData")
    paths = write_rca_report(summary, report_root=tmp_path / "rca")
    markdown = paths["summary_md"].read_text(encoding="utf-8")

    assert summary["overall_status"] == "BLOCK"
    assert market_data["status"] == "BLOCK"
    assert "MARKET_PROJECTION_ERROR" in market_data["reason_codes"]
    assert "evt_bad_tick" in markdown
    assert "invalid price" in markdown
    assert '"price": 0' in markdown


def test_rca_keeps_historical_projection_errors_out_of_current_reasons() -> None:
    results = _healthy_results()
    results["market_data_status"]["data"]["projection_error_count"] = 7
    results["market_data_status"]["data"]["recent_projection_error_count"] = 0
    results["market_data_projection_errors"]["data"]["errors"] = [
        {"event_id": "evt_old_tick", "error_message": "old invalid price"}
    ]

    summary = classify_market_open_rca(results, generated_at=_now())
    market_data = _stage(summary, "MarketData")

    assert summary["overall_status"] == "WARN"
    assert market_data["status"] == "WARN"
    assert "MARKET_PROJECTION_ERROR" not in market_data["reason_codes"]
    assert "Historical market projection errors exist: 7" in market_data["summary"]


def test_rca_classifies_order_command_count_as_block() -> None:
    results = _healthy_results()
    results["gateway_commands_status"]["data"]["command_type_counts"] = {"send_order": 1}
    results["gateway_commands_status"]["data"]["order_command_count"] = 1

    summary = classify_market_open_rca(results, generated_at=_now())
    order_safety = _stage(summary, "OrderSafety")

    assert summary["overall_status"] == "BLOCK"
    assert order_safety["status"] == "BLOCK"
    assert "ORDER_COMMAND_ZERO_EXPECTED" in order_safety["reason_codes"]


def test_rca_classifies_gateway_condition_and_realtime_reasons() -> None:
    results = _healthy_results()
    results["gateway_events_recent"]["data"]["events"] = [
        {
            "event_type": "heartbeat",
            "event_ts": _now(),
            "payload": {"registered_realtime_code_count": 0},
        },
        {
            "event_type": "condition_event",
            "event_ts": _now(),
            "payload": {"action": "ENTER", "code": "005930"},
        },
        {
            "event_type": "condition_load_result",
            "event_ts": _now(),
            "payload": {"success": False, "message": "GetConditionLoad failed"},
        },
    ]

    summary = classify_market_open_rca(results, generated_at=_now())
    gateway = _stage(summary, "Gateway")

    assert gateway["status"] == "BLOCK"
    assert "CONDITION_LOAD_FAILED" in gateway["reason_codes"]
    assert "REALTIME_NOT_REGISTERED" in gateway["reason_codes"]


def test_rca_classifies_registered_realtime_without_callbacks_as_block() -> None:
    results = _healthy_results()
    results["gateway_events_recent"]["data"]["events"] = [
        {
            "event_type": "heartbeat",
            "event_ts": _now(),
            "payload": {
                "kiwoom_logged_in": True,
                "registered_realtime_code_count": 2,
                "latest_realtime_registration_at": _now(),
                "latest_realtime_callback_at": "",
                "realtime_callback_count": 0,
                "realtime_recover_count": 2,
            },
        },
    ]

    summary = classify_market_open_rca(results, generated_at=_now())
    gateway = _stage(summary, "Gateway")

    assert summary["overall_status"] == "BLOCK"
    assert gateway["status"] == "BLOCK"
    assert "REALTIME_CALLBACK_MISSING" in gateway["reason_codes"]


def test_rca_classifies_comm_connect_no_return_as_block() -> None:
    results = _healthy_results()
    results["gateway_events_recent"]["data"]["events"] = [
        {
            "event_type": "heartbeat",
            "event_ts": _now(),
            "payload": {
                "kiwoom_logged_in": False,
                "login_requested": True,
                "login_in_progress": False,
                "comm_connect_state": "EVENT_TIMEOUT_NO_COMM_CONNECT_RESULT",
                "login_block_reason_codes": [
                    "COMM_CONNECT_NO_RETURN",
                    "ON_EVENT_CONNECT_TIMEOUT",
                    "KIWOOM_LOGIN_DIALOG_OR_VERSION_SUSPECTED",
                ],
            },
        },
    ]

    summary = classify_market_open_rca(results, generated_at=_now())
    gateway = _stage(summary, "Gateway")

    assert summary["overall_status"] == "BLOCK"
    assert gateway["status"] == "BLOCK"
    assert "COMM_CONNECT_NO_RETURN" in gateway["reason_codes"]
    assert "ON_EVENT_CONNECT_TIMEOUT" in gateway["reason_codes"]


def _healthy_results() -> dict[str, dict[str, object]]:
    now = _now()
    return {
        "health": _ok("Core", {"status": "ok"}),
        "api_status": _ok(
            "Core",
            {
                "status": "ok",
                "mode": "OBSERVE",
                "live_sim_allowed": False,
                "live_real_allowed": False,
            },
        ),
        "gateway_auth_probe": _ok("Gateway", {"authenticated": True}),
        "gateway_status": _ok(
            "Gateway",
            {
                "last_heartbeat_at": now,
                "recent_event_count": 3,
                "queued_command_count": 0,
                "failed_command_count": 0,
            },
        ),
        "gateway_events_recent": _ok(
            "Gateway",
            {
                "events": [
                    {
                        "event_type": "heartbeat",
                        "event_ts": now,
                        "payload": {
                            "kiwoom_logged_in": True,
                            "registered_realtime_code_count": 1,
                            "condition_load_state": "LOADED",
                        },
                    },
                    {"event_type": "price_tick", "event_ts": now, "payload": {"code": "005930"}},
                    {
                        "event_type": "condition_event",
                        "event_ts": now,
                        "payload": {"code": "005930", "action": "ENTER"},
                    },
                ]
            },
        ),
        "gateway_commands_status": _ok(
            "Gateway",
            {
                "counts": {"QUEUED": 0, "FAILED": 0, "REJECTED": 0},
                "command_type_counts": {},
                "order_command_count": 0,
            },
        ),
        "market_data_status": _ok(
            "MarketData",
            {"latest_tick_count": 1, "projection_error_count": 0, "tick_stale_sec": 999999},
        ),
        "market_data_ticks_latest": _ok(
            "MarketData",
            {"ticks": [{"code": "005930", "event_ts": now}]},
        ),
        "market_data_projection_errors": _ok("MarketData", {"errors": []}),
        "market_data_conditions_recent": _ok(
            "MarketData",
            {"signals": [{"code": "005930", "event_ts": now}]},
        ),
        "themes_status": _ok(
            "Theme",
            {
                "theme_count": 1,
                "active_theme_count": 1,
                "member_count": 2,
                "latest_snapshot_count": 1,
                "projection_error_count": 0,
            },
        ),
        "themes": _ok("Theme", {"themes": [{"theme_id": "semiconductor"}]}),
        "themes_snapshots_latest": _ok(
            "Theme",
            {"snapshots": [{"theme_id": "semiconductor", "state": "LEADING"}]},
        ),
        "themes_projection_errors": _ok("Theme", {"errors": []}),
        "candidates_status": _ok(
            "Candidate",
            {
                "candidate_count": 1,
                "active_candidate_count": 1,
                "state_counts": {"DATA_WAIT": 0},
                "projection_error_count": 0,
            },
        ),
        "candidates": _ok("Candidate", {"candidates": [{"candidate_instance_id": "CAND-1"}]}),
        "candidates_projection_errors": _ok("Candidate", {"errors": []}),
        "strategy_status": _ok("Strategy", {"latest_observation_count": 1}),
        "strategy_runs": _ok("Strategy", {"runs": [{"run_id": "strategy_run_1"}]}),
        "strategy_errors": _ok("Strategy", {"errors": []}),
        "risk_status": _ok("Risk", {"latest_observation_count": 1}),
        "risk_runs": _ok("Risk", {"runs": [{"run_id": "risk_run_1"}]}),
        "risk_errors": _ok("Risk", {"errors": []}),
        "entry_timing_status": _ok(
            "EntryTiming",
            {
                "evaluation_count": 1,
                "latest_plan_count": 1,
                "plan_ready_count": 1,
                "data_wait_count": 0,
                "error_count": 0,
            },
        ),
        "entry_timing_plans_latest": _ok(
            "EntryTiming",
            {"order_plan_drafts": [{"order_plan_id": "plan_1", "status": "PLAN_READY"}]},
        ),
        "entry_timing_errors": _ok("EntryTiming", {"errors": []}),
        "live_sim_status": _ok(
            "LiveSim",
            {"enabled": False, "kill_switch": True, "order_count": 0},
        ),
        "live_sim_operator_status": _ok("LiveSim", {"blocking_reasons": []}),
        "live_sim_operator_run_latest": _ok("LiveSim", {"run": None}),
        "live_sim_rejections": _ok("LiveSim", {"rejections": []}),
        "live_sim_errors": _ok("LiveSim", {"errors": []}),
        "live_sim_reconcile_latest": _ok("LiveSim", {"reconcile": None}),
    }


def _ok(stage: str, data: dict[str, object]) -> dict[str, object]:
    return {"ok": True, "status_code": 200, "stage": stage, "endpoint": "", "data": data}


def _fail(
    stage: str,
    endpoint: str,
    *,
    status_code: int | None,
    error: str,
) -> dict[str, object]:
    return {
        "ok": False,
        "status_code": status_code,
        "stage": stage,
        "endpoint": endpoint,
        "error": error,
        "data": {},
    }


def _stage(summary: dict[str, object], stage_name: str) -> dict[str, object]:
    stages = summary["stages"]
    assert isinstance(stages, list)
    for stage in stages:
        if stage["stage"] == stage_name:
            return stage
    raise AssertionError(stage_name)


def _now() -> str:
    return datetime.now(tz=UTC).isoformat()
