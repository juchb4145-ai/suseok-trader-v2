from __future__ import annotations

from tools import ops_market_data_condition_event_cutover_check as tool


def test_ops_condition_event_cutover_check_passes_normal_skip() -> None:
    report = _base_report(
        routing_overrides={
            "condition_event_cutover_enabled": True,
            "condition_event_effective_skip_count": 2,
            "condition_event_worker_applied_count": 2,
            "condition_event_deferred_fusion_refresh_count": 2,
            "condition_event_skip_budget_remaining_current_minute": 1,
            "append_only_ready": True,
            "worker_apply_enabled": True,
        },
        dashboard_routing_overrides={"condition_event_effective_skip_count": 2},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []


def test_ops_condition_event_cutover_check_fails_bad_worker_evidence() -> None:
    report = _base_report(
        routing_overrides={
            "condition_event_cutover_enabled": True,
            "condition_event_effective_skip_count": 1,
            "condition_event_candidate_ingest_executed_count": 1,
            "condition_event_deferred_fusion_refresh_error_count": 1,
            "condition_event_artifact_missing_after_worker_count": 1,
            "append_only_ready": True,
            "worker_apply_enabled": True,
        },
        outbox_overrides={"error_count": 1},
        dashboard_routing_overrides={"condition_event_effective_skip_count": 1},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "CONDITION_EVENT_CANDIDATE_INGEST_IN_WORKER" in verdict["failures"]
    assert "CONDITION_EVENT_DEFERRED_FUSION_REFRESH_ERROR" in verdict["failures"]
    assert "CONDITION_EVENT_ARTIFACT_MISSING_AFTER_WORKER" in verdict["failures"]
    assert "PROJECTION_OUTBOX_ERROR_OR_DEAD_LETTER" in verdict["failures"]


def test_ops_condition_event_cutover_check_warns_without_skip_observation() -> None:
    report = _base_report(
        routing_overrides={
            "condition_event_cutover_enabled": True,
            "condition_event_effective_skip_count": 0,
            "condition_event_deferred_fusion_refresh_count": 0,
            "condition_event_skip_budget_remaining_current_minute": 0,
        },
        dashboard_routing_overrides={"condition_event_effective_skip_count": 0},
    )

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "NO_CONDITION_EVENT_EFFECTIVE_SKIP_OBSERVED" in verdict["warnings"]
    assert "CONDITION_EVENT_SKIP_BUDGET_EXHAUSTED" in verdict["warnings"]


def _base_report(
    *,
    routing_overrides: dict[str, object] | None = None,
    outbox_overrides: dict[str, object] | None = None,
    dashboard_routing_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    routing = {
        "condition_event_cutover_enabled": True,
        "condition_event_skip_budget_remaining_current_minute": 10,
        "condition_event_effective_skip_count": 1,
        "condition_event_pending_worker_count": 0,
        "condition_event_worker_applied_count": 1,
        "condition_event_deferred_fusion_refresh_count": 1,
        "condition_event_deferred_fusion_refresh_error_count": 0,
        "condition_event_candidate_ingest_executed_count": 0,
        "condition_event_artifact_missing_after_worker_count": 0,
        "append_only_ready": True,
        "worker_apply_enabled": True,
    }
    routing.update(routing_overrides or {})
    outbox = {"pending_count": 0, "error_count": 0, "dead_letter_count": 0}
    outbox.update(outbox_overrides or {})
    dashboard_routing = {
        "condition_event_effective_skip_count": (
            routing["condition_event_effective_skip_count"]
        )
    }
    dashboard_routing.update(dashboard_routing_overrides or {})
    return {
        "routing_status": {"ok": True, "data": routing},
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
