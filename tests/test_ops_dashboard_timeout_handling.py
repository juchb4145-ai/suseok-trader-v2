from __future__ import annotations

from tools import ops_market_data_condition_event_side_effect_check as tool


def test_ops_condition_event_dashboard_timeout_is_warn_not_core_fail() -> None:
    report = _base_report()
    report["dashboard_snapshot"] = {
        "ok": False,
        "status_code": None,
        "error": "timed out",
        "data": {},
    }

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["failures"] == []
    assert "DASHBOARD_SNAPSHOT_API_ERROR" in verdict["warnings"]
    assert verdict["block_next_pr"] is True


def test_ops_condition_event_core_fail_remains_fail_even_if_dashboard_passes() -> None:
    report = _base_report()
    report["routing_status"] = {"ok": False, "status_code": 500, "data": {}}

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "ROUTING_STATUS_API_ERROR" in verdict["failures"]
    assert verdict["block_next_pr"] is True


def test_ops_condition_event_core_and_dashboard_passes() -> None:
    verdict = tool.evaluate_report(_base_report())

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []
    assert verdict["block_next_pr"] is False


def test_ops_condition_event_dashboard_timeout_budget_skip_blocks_next_pr() -> None:
    report = _base_report()
    report["dashboard_snapshot"]["data"]["warnings"] = [
        "SKIPPED_TIMEOUT_BUDGET:errors"
    ]
    report["dashboard_snapshot"]["data"]["skipped_sections"] = [
        {"section": "errors", "reason": "SKIPPED_TIMEOUT_BUDGET"}
    ]

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "DASHBOARD_SECTION_SKIPPED_TIMEOUT_BUDGET" in verdict["warnings"]
    assert verdict["block_next_pr"] is True


def _base_report() -> dict[str, object]:
    routing = {
        "condition_event_worker_side_effect_ready": True,
        "condition_event_fusion_enabled": True,
        "condition_event_would_skip_inline_count": 1,
        "condition_event_effective_skip_count": 0,
        "condition_event_deferred_fusion_refresh_count": 1,
        "condition_event_deferred_fusion_refresh_error_count": 0,
        "condition_event_candidate_ingest_executed_count": 0,
        "condition_event_side_effect_duplicate_count": 0,
        "invalid_effective_skip_count": 0,
    }
    return {
        "routing_status": {"ok": True, "data": routing},
        "projection_outbox": {
            "ok": True,
            "data": {"pending_count": 0, "error_count": 0, "dead_letter_count": 0},
        },
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
                "total_latency_ms": 10,
                "warnings": [],
                "skipped_sections": [],
                "pipeline_summary": {
                    "market_data_append_only_routing": {
                        "condition_event_effective_skip_count": 0,
                    },
                },
            },
        },
    }
