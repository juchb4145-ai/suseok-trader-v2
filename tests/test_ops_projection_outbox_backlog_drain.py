from __future__ import annotations

from tools.ops_projection_outbox_backlog_drain import evaluate_report


def test_ops_backlog_drain_pending_decrease_is_warn_not_fail() -> None:
    report = {
        "drain_requested": True,
        "initial_backlog": _payload(
            {
                "readiness_status": "WARN",
                "pending_count": 100,
                "pr11_condition_event_cutover_ready": False,
            }
        ),
        "final_backlog": _payload(
            {
                "readiness_status": "WARN",
                "pending_count": 50,
                "recent_pending_count": 0,
                "condition_event_pending_count": 0,
                "stale_processing_count": 0,
                "error_count": 0,
                "dead_letter_count": 0,
                "pr11_condition_event_cutover_ready": False,
            }
        ),
        "latest_reconcile": _payload(
            {"latest_run": {"run_id": "reconcile_test", "status": "PASS"}}
        ),
        "drain_payloads": [_payload({"status": "COMPLETED", "pending_delta": -50})],
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["failures"] == []
    assert "PROJECTION_OUTBOX_PENDING_DECREASED_BUT_REMAINS" in verdict["warnings"]


def test_ops_backlog_drain_error_dead_letter_is_fail() -> None:
    report = _base_report(
        {
            "readiness_status": "FAIL",
            "pending_count": 0,
            "error_count": 1,
            "dead_letter_count": 1,
            "stale_processing_count": 0,
            "pr11_condition_event_cutover_ready": False,
        }
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "PROJECTION_OUTBOX_ERROR" in verdict["failures"]
    assert "PROJECTION_OUTBOX_DEAD_LETTER" in verdict["failures"]
    assert verdict["block_next_pr"] is True


def test_ops_backlog_drain_condition_event_backlog_blocks_next_pr() -> None:
    report = _base_report(
        {
            "readiness_status": "FAIL",
            "pending_count": 200,
            "condition_event_pending_count": 150,
            "error_count": 0,
            "dead_letter_count": 0,
            "stale_processing_count": 0,
            "pr11_condition_event_cutover_ready": False,
        }
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "PROJECTION_OUTBOX_BACKLOG_READINESS_FAIL" in verdict["failures"]
    assert verdict["block_next_pr"] is True


def _base_report(final_backlog: dict) -> dict:
    return {
        "drain_requested": False,
        "initial_backlog": _payload(final_backlog),
        "final_backlog": _payload(final_backlog),
        "latest_reconcile": _payload(
            {"latest_run": {"run_id": "reconcile_test", "status": "PASS"}}
        ),
        "drain_payloads": [],
    }


def _payload(data: dict) -> dict:
    return {"ok": True, "status_code": 200, "data": data}
