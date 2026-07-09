from __future__ import annotations

from tools.ops_market_data_append_only_controller_check import evaluate_report


def test_ops_controller_check_fails_on_auto_rollback() -> None:
    report = _report(
        controller={
            "auto_rollback_required": True,
            "auto_rollback_reason_codes": ["PROJECTION_OUTBOX_ERROR_COUNT_EXCEEDED"],
            "operating_mode": "MARKET_DATA_LIMITED",
            "global_kill_switch": False,
            "global_skip_budget_remaining": 5,
            "backlog_readiness_status": "PASS",
            "invalid_effective_skip_count": 0,
            "allowed_event_types": ["price_tick", "tr_response", "condition_event"],
            "status": "FAIL",
            "reason_codes": [],
        },
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_REQUIRED" in verdict["failures"]
    assert verdict["block_next_pr"] is True


def test_ops_controller_check_warns_for_default_off_mode() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "WARN"
    assert "OPERATING_MODE_OFF" in verdict["warnings"]
    assert "GLOBAL_KILL_SWITCH_ACTIVE" in verdict["warnings"]
    assert "GLOBAL_SKIP_BUDGET_EXHAUSTED" in verdict["warnings"]
    assert verdict["failures"] == []


def test_ops_controller_check_passes_healthy_limited_mode() -> None:
    report = _report(
        controller={
            "auto_rollback_required": False,
            "operating_mode": "MARKET_DATA_LIMITED",
            "global_kill_switch": False,
            "global_skip_budget_remaining": 5,
            "backlog_readiness_status": "PASS",
            "invalid_effective_skip_count": 0,
            "allowed_event_types": ["price_tick", "tr_response", "condition_event"],
            "status": "PASS",
            "reason_codes": [],
        },
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []


def _report(*, controller: dict | None = None) -> dict:
    controller_payload = {
        "auto_rollback_required": False,
        "operating_mode": "OFF",
        "global_kill_switch": True,
        "global_skip_budget_remaining": 0,
        "backlog_readiness_status": "PASS",
        "invalid_effective_skip_count": 0,
        "allowed_event_types": [],
        "status": "WARN",
        "reason_codes": [],
    }
    if controller:
        controller_payload.update(controller)
    return {
        "controller_status": {"ok": True, "data": controller_payload},
        "routing_status": {
            "ok": True,
            "data": {
                "effective_price_tick_skip_count": 0,
                "tr_response_effective_skip_count": 0,
                "condition_event_effective_skip_count": 0,
                "condition_event_candidate_ingest_executed_count": 0,
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {"error_count": 0, "dead_letter_count": 0},
        },
        "projection_outbox_backlog": {
            "ok": True,
            "data": {"readiness_status": "PASS"},
        },
        "latest_reconcile": {
            "ok": True,
            "data": {"latest_run": {"status": "PASS"}},
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "pipeline_summary": {
                    "market_data_append_only_controller": {
                        "status": controller_payload["status"],
                    }
                }
            },
        },
        "snapshot": {},
    }
