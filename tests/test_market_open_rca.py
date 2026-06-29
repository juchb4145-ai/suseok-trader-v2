from __future__ import annotations

from datetime import UTC, datetime

from tools.ops_market_open_rca import classify_market_open_rca, write_rca_report


def test_rca_classifies_theme_membership_empty(tmp_path) -> None:
    results = _healthy_results()
    results["themes_status"]["data"] = {
        "theme_count": 0,
        "active_theme_count": 0,
        "member_count": 0,
        "latest_snapshot_count": 0,
        "projection_error_count": 0,
    }
    results["themes"]["data"] = {"themes": []}
    results["themes_snapshots_latest"]["data"] = {"snapshots": []}

    summary = classify_market_open_rca(
        results,
        core_url="http://127.0.0.1:8000",
        trade_date="2026-06-29",
        generated_at=_now(),
    )
    theme = _stage(summary, "Theme")
    paths = write_rca_report(summary, report_root=tmp_path / "reports")

    assert summary["overall_status"] == "BLOCK"
    assert theme["status"] == "BLOCK"
    assert "THEME_MEMBERSHIP_EMPTY" in theme["reason_codes"]
    assert paths["summary_md"].exists()
    assert "THEME_MEMBERSHIP_EMPTY" in paths["summary_md"].read_text(encoding="utf-8")


def test_rca_classifies_gateway_auth_failure() -> None:
    results = _healthy_results()
    results["gateway_auth_probe"] = {
        "ok": False,
        "status_code": 401,
        "stage": "Gateway",
        "endpoint": "/api/gateway/auth/probe",
        "error": "Gateway token is required",
        "data": {"detail": "Gateway token is required"},
    }

    summary = classify_market_open_rca(
        results,
        core_url="http://127.0.0.1:8000",
        trade_date="2026-06-29",
        generated_at=_now(),
    )
    gateway = _stage(summary, "Gateway")

    assert summary["overall_status"] == "BLOCK"
    assert gateway["status"] == "BLOCK"
    assert "GATEWAY_AUTH_FAILED" in gateway["reason_codes"]


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
                "recent_event_count": 1,
                "queued_command_count": 0,
                "failed_command_count": 0,
            },
        ),
        "gateway_events_recent": _ok("Gateway", {"events": [{"event_ts": now}]}),
        "gateway_commands_status": _ok("Gateway", {"counts": {"QUEUED": 0, "FAILED": 0}}),
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
            {"conditions": [{"code": "005930", "event_ts": now}]},
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
        "themes_snapshots_latest": _ok("Theme", {"snapshots": [{"theme_id": "semiconductor"}]}),
        "themes_projection_errors": _ok("Theme", {"errors": []}),
        "candidates_status": _ok(
            "Candidate",
            {"candidate_count": 1, "active_candidate_count": 1, "projection_error_count": 0},
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
            {"evaluation_count": 1, "latest_plan_count": 1, "error_count": 0},
        ),
        "entry_timing_plans_latest": _ok(
            "EntryTiming",
            {"order_plan_drafts": [{"order_plan_id": "plan_1"}]},
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


def _stage(summary: dict[str, object], stage_name: str) -> dict[str, object]:
    stages = summary["stages"]
    assert isinstance(stages, list)
    for stage in stages:
        if stage["stage"] == stage_name:
            return stage
    raise AssertionError(stage_name)


def _now() -> str:
    return datetime.now(tz=UTC).isoformat()
