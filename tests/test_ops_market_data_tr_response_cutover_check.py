from __future__ import annotations

from tools import ops_market_data_tr_response_cutover_check as tool


def test_ops_tr_response_cutover_check_passes_normal_cutover() -> None:
    verdict = tool.evaluate_report(
        _base_report(
            routing_overrides={
                "tr_response_effective_skip_count": 2,
                "tr_response_worker_applied_count": 2,
                "tr_response_deferred_quote_refresh_count": 2,
                "tr_response_skip_budget_remaining_current_minute": 1,
                "append_only_ready": True,
                "worker_apply_enabled": True,
            },
            dashboard_routing_overrides={"tr_response_effective_skip_count": 2},
        )
    )

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []


def test_ops_tr_response_cutover_check_fails_invalid_skip_or_errors() -> None:
    verdict = tool.evaluate_report(
        _base_report(
            routing_overrides={
                "tr_response_effective_skip_count": 1,
                "condition_event_effective_skip_count": 1,
                "invalid_effective_skip_count": 1,
                "tr_response_deferred_quote_refresh_error_count": 1,
                "append_only_ready": True,
                "worker_apply_enabled": True,
            },
            outbox_overrides={"error_count": 1},
            dashboard_routing_overrides={"tr_response_effective_skip_count": 1},
        )
    )

    assert verdict["status"] == "FAIL"
    assert "INVALID_EFFECTIVE_SKIP_EVENT_TYPE" in verdict["failures"]
    assert "TR_RESPONSE_DEFERRED_QUOTE_REFRESH_ERROR" in verdict["failures"]
    assert "PROJECTION_OUTBOX_ERROR_OR_DEAD_LETTER" in verdict["failures"]


def _base_report(
    *,
    routing_overrides: dict[str, object] | None = None,
    outbox_overrides: dict[str, object] | None = None,
    dashboard_routing_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    routing = {
        "tr_response_cutover_enabled": True,
        "tr_response_skip_budget_remaining_current_minute": 10,
        "tr_response_effective_skip_count": 1,
        "tr_response_pending_worker_count": 0,
        "tr_response_worker_applied_count": 1,
        "tr_response_deferred_quote_refresh_count": 1,
        "tr_response_deferred_quote_refresh_error_count": 0,
        "condition_event_effective_skip_count": 0,
        "invalid_effective_skip_count": 0,
        "append_only_ready": True,
        "worker_apply_enabled": True,
    }
    routing.update(routing_overrides or {})
    outbox = {"pending_count": 0, "error_count": 0, "dead_letter_count": 0}
    outbox.update(outbox_overrides or {})
    dashboard_routing = {
        "tr_response_effective_skip_count": routing["tr_response_effective_skip_count"]
    }
    dashboard_routing.update(dashboard_routing_overrides or {})
    return {
        "routing_status": {"ok": True, "data": routing},
        "routing_decisions": {"ok": True, "data": {"decisions": []}},
        "projection_outbox": {"ok": True, "data": outbox},
        "projection_outbox_backlog": {
            "ok": True,
            "data": {
                "readiness_status": "PASS",
                "pr11_condition_event_cutover_ready": True,
                "operator_actions": [],
            },
        },
        "latest_reconcile": {
            "ok": True,
            "data": {
                "latest_run": {
                    "status": "PASS",
                    "append_only_ready": True,
                    "checked_event_count": 2,
                }
            },
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "pipeline_summary": {
                    "market_data_append_only_routing": dashboard_routing,
                }
            },
        },
    }
