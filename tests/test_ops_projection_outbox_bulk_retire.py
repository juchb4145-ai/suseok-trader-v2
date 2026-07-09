from __future__ import annotations

from tools.ops_projection_outbox_bulk_retire import evaluate_report


def test_ops_bulk_retire_dry_run_with_eligible_jobs_is_pass() -> None:
    report = {
        "dry_run": True,
        "before_backlog": _payload(
            {
                "pending_count": 10,
                "error_count": 0,
                "dead_letter_count": 0,
                "readiness_status": "WARN",
                "blocking_pending_count": 0,
                "bulk_retire_eligible_count": 10,
                "pr11_condition_event_cutover_ready": True,
            }
        ),
        "bulk_retire": _payload(
            {
                "status": "COMPLETED",
                "retired_count": 10,
                "applied_count": 10,
                "skipped_count": 0,
                "pending_delta": 0,
            }
        ),
        "after_backlog": _payload(
            {
                "pending_count": 10,
                "error_count": 0,
                "dead_letter_count": 0,
                "readiness_status": "WARN",
                "blocking_pending_count": 0,
                "bulk_retire_eligible_count": 10,
                "pr11_condition_event_cutover_ready": True,
            }
        ),
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []
    assert verdict["block_next_pr"] is False


def test_ops_bulk_retire_apply_pending_decrease_is_pass() -> None:
    report = {
        "dry_run": False,
        "before_backlog": _payload(
            {
                "pending_count": 10,
                "error_count": 0,
                "dead_letter_count": 0,
                "readiness_status": "WARN",
                "blocking_pending_count": 0,
                "bulk_retire_eligible_count": 10,
                "pr11_condition_event_cutover_ready": True,
            }
        ),
        "bulk_retire": _payload(
            {
                "status": "COMPLETED",
                "retired_count": 5,
                "applied_count": 4,
                "skipped_count": 1,
                "pending_delta": -5,
            }
        ),
        "after_backlog": _payload(
            {
                "pending_count": 5,
                "error_count": 0,
                "dead_letter_count": 0,
                "readiness_status": "PASS",
                "blocking_pending_count": 0,
                "bulk_retire_eligible_count": 0,
                "pr11_condition_event_cutover_ready": True,
            }
        ),
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["warnings"] == []
    assert verdict["block_next_pr"] is False


def test_ops_bulk_retire_error_dead_letter_is_fail() -> None:
    report = {
        "dry_run": False,
        "before_backlog": _payload(
            {
                "pending_count": 10,
                "error_count": 0,
                "dead_letter_count": 0,
                "readiness_status": "WARN",
                "blocking_pending_count": 0,
                "bulk_retire_eligible_count": 10,
                "pr11_condition_event_cutover_ready": False,
            }
        ),
        "bulk_retire": _payload(
            {
                "status": "COMPLETED",
                "retired_count": 1,
                "applied_count": 1,
                "skipped_count": 0,
                "pending_delta": -1,
            }
        ),
        "after_backlog": _payload(
            {
                "pending_count": 9,
                "error_count": 1,
                "dead_letter_count": 1,
                "readiness_status": "FAIL",
                "blocking_pending_count": 0,
                "bulk_retire_eligible_count": 0,
                "pr11_condition_event_cutover_ready": False,
            }
        ),
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "PROJECTION_OUTBOX_ERROR" in verdict["failures"]
    assert "PROJECTION_OUTBOX_DEAD_LETTER" in verdict["failures"]
    assert verdict["block_next_pr"] is True


def _payload(data: dict) -> dict:
    return {"ok": True, "status_code": 200, "data": data}
