from tools.ops_incremental_evaluation_queue_check import evaluate_report


def _report(*, status: str = "PASS", dead_letter_count: int = 0):
    return {
        "requested_actions": {
            "sweep_retry_exhausted": False,
            "reset_dead_letter_id": None,
        },
        "core_status": {
            "mode": "OBSERVE",
            "live_sim_allowed": False,
            "live_real_allowed": False,
        },
        "command_status_before": {
            "order_commands_allowed": False,
            "order_command_count": 0,
            "counts": {"ACKED": 2},
        },
        "command_status_after": {
            "order_commands_allowed": False,
            "order_command_count": 0,
            "counts": {"ACKED": 2},
        },
        "queue_status_before": {
            "status": status,
            "queued_count": 0,
            "retry_exhausted_count": 0,
            "dead_letter_count": dead_letter_count,
            "stale_queue_count": 0,
            "reason_codes": [],
        },
        "queue_status_after": {
            "status": status,
            "queued_count": 0,
            "retry_exhausted_count": 0,
            "dead_letter_count": dead_letter_count,
            "stale_queue_count": 0,
            "reason_codes": [],
        },
        "dead_letters_before": {"count": dead_letter_count, "items": []},
        "dead_letters_after": {"count": dead_letter_count, "items": []},
        "dashboard_snapshot": {
            "incremental_evaluation": {"queued_count": 0}
        },
    }


def test_ops_incremental_queue_passes_healthy_read_only_check() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["read_only_by_default"] is True
    assert verdict["command_count_delta"] == 0
    assert verdict["order_command_count_delta"] == 0


def test_ops_incremental_queue_fails_when_dead_letter_remains() -> None:
    verdict = evaluate_report(_report(status="FAIL", dead_letter_count=1))

    assert verdict["status"] == "FAIL"
    assert "INCREMENTAL_QUEUE_STATUS_FAIL" in verdict["failures"]
    assert "INCREMENTAL_DEAD_LETTER_PRESENT" in verdict["failures"]
