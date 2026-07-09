from __future__ import annotations

from tools import ops_market_data_price_tick_cutover_check as tool


def test_ops_price_tick_cutover_check_passes_price_tick_only_skip() -> None:
    report = _base_report(
        routing_overrides={
            "effective_skip_inline_count": 2,
            "effective_price_tick_skip_count": 2,
            "deferred_incremental_enqueue_count": 2,
            "worker_apply_enabled": True,
            "append_only_ready": True,
        },
        outbox_overrides={"pending_count": 0, "error_count": 0, "dead_letter_count": 0},
        dashboard_routing_overrides={"invalid_effective_skip_count": 0},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []


def test_ops_price_tick_cutover_check_fails_invalid_effective_skip() -> None:
    report = _base_report(
        routing_overrides={
            "effective_skip_inline_count": 2,
            "effective_price_tick_skip_count": 1,
            "condition_event_effective_skip_count": 1,
            "invalid_effective_skip_count": 1,
            "deferred_incremental_enqueue_count": 1,
            "worker_apply_enabled": True,
            "append_only_ready": True,
        },
        dashboard_routing_overrides={"invalid_effective_skip_count": 1},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "INVALID_EFFECTIVE_SKIP_EVENT_TYPE" in verdict["failures"]


def test_ops_price_tick_cutover_check_warns_when_worker_pending() -> None:
    report = _base_report(
        routing_overrides={
            "effective_skip_inline_count": 1,
            "effective_price_tick_skip_count": 1,
            "deferred_incremental_enqueue_count": 0,
            "worker_apply_enabled": True,
            "append_only_ready": True,
        },
        outbox_overrides={"pending_count": 1, "error_count": 0, "dead_letter_count": 0},
        dashboard_routing_overrides={"invalid_effective_skip_count": 0},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "DEFERRED_INCREMENTAL_ENQUEUE_WAITING_FOR_WORKER" in verdict["warnings"]
    assert "PROJECTION_OUTBOX_PENDING_WORKER_RUN_ONCE_RECOMMENDED" in verdict["warnings"]


def _base_report(
    *,
    routing_overrides: dict[str, object] | None = None,
    outbox_overrides: dict[str, object] | None = None,
    dashboard_routing_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    routing = {
        "dry_run_enabled": True,
        "cutover_enabled": True,
        "price_tick_cutover_enabled": True,
        "skip_budget_limit_per_minute": 10,
        "skip_budget_remaining_current_minute": 8,
        "effective_skip_inline_count": 0,
        "effective_price_tick_skip_count": 0,
        "condition_event_effective_skip_count": 0,
        "tr_response_effective_skip_count": 0,
        "invalid_effective_skip_count": 0,
        "effective_skip_outbox_error_count": 0,
        "deferred_incremental_enqueue_count": 0,
        "worker_apply_enabled": True,
        "append_only_ready": True,
    }
    routing.update(routing_overrides or {})
    outbox = {"pending_count": 0, "error_count": 0, "dead_letter_count": 0}
    outbox.update(outbox_overrides or {})
    dashboard_routing = {
        "invalid_effective_skip_count": routing["invalid_effective_skip_count"]
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
