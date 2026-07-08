from __future__ import annotations

from tools import ops_market_data_tr_response_side_effect_check as tool


def test_ops_tr_response_side_effect_check_passes_ready_state() -> None:
    report = _base_report(
        routing_overrides={
            "tr_response_would_skip_inline_count": 2,
            "tr_response_deferred_side_effect_count": 2,
            "tr_response_worker_side_effect_ready": True,
            "append_only_ready": True,
        },
        outbox_overrides={"pending_count": 0, "error_count": 0, "dead_letter_count": 0},
        dashboard_routing_overrides={"tr_response_effective_skip_count": 0},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []


def test_ops_tr_response_side_effect_check_fails_forbidden_skip_and_errors() -> None:
    report = _base_report(
        routing_overrides={
            "tr_response_effective_skip_count": 1,
            "condition_event_effective_skip_count": 1,
            "invalid_effective_skip_count": 1,
            "tr_response_deferred_side_effect_error_count": 1,
            "tr_response_duplicate_side_effect_count": 1,
        },
        outbox_overrides={"error_count": 1, "dead_letter_count": 0},
        dashboard_routing_overrides={"tr_response_effective_skip_count": 1},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "TR_RESPONSE_EFFECTIVE_SKIP_FORBIDDEN" in verdict["failures"]
    assert "CONDITION_EVENT_EFFECTIVE_SKIP_FORBIDDEN" in verdict["failures"]
    assert "INVALID_EFFECTIVE_SKIP_EVENT_TYPE" in verdict["failures"]
    assert "TR_RESPONSE_DEFERRED_SIDE_EFFECT_ERROR" in verdict["failures"]
    assert "TR_RESPONSE_DUPLICATE_QUOTE_REFRESH_SIDE_EFFECT" in verdict["failures"]
    assert "PROJECTION_OUTBOX_ERROR_OR_DEAD_LETTER" in verdict["failures"]


def test_ops_tr_response_side_effect_check_warns_without_tr_events() -> None:
    report = _base_report(
        routing_overrides={
            "tr_response_would_skip_inline_count": 0,
            "tr_response_deferred_side_effect_count": 0,
            "tr_response_worker_side_effect_ready": False,
        },
        dashboard_routing_overrides={"tr_response_effective_skip_count": 0},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "NO_TR_RESPONSE_EVENTS_OBSERVED" in verdict["warnings"]
    assert "TR_RESPONSE_WORKER_SIDE_EFFECT_NOT_READY" in verdict["warnings"]


def _base_report(
    *,
    routing_overrides: dict[str, object] | None = None,
    outbox_overrides: dict[str, object] | None = None,
    dashboard_routing_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    routing = {
        "dry_run_enabled": True,
        "cutover_enabled": True,
        "append_only_ready": True,
        "tr_response_dry_run_enabled": True,
        "tr_response_cutover_enabled": False,
        "tr_response_worker_side_effect_ready": True,
        "tr_response_would_skip_inline_count": 1,
        "tr_response_effective_skip_count": 0,
        "condition_event_effective_skip_count": 0,
        "invalid_effective_skip_count": 0,
        "tr_response_deferred_side_effect_count": 1,
        "tr_response_deferred_side_effect_error_count": 0,
        "tr_response_duplicate_side_effect_count": 0,
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
