from tools.ops_live_sim_lifecycle_consumer_check import evaluate_report


def _report(*, dead_letter_count: int = 0, command_delta: int = 0):
    before_total = 4
    return {
        "core_status": {
            "mode": "OBSERVE",
            "live_sim_allowed": False,
            "live_real_allowed": False,
        },
        "command_status_before": {
            "order_commands_allowed": False,
            "order_command_count": 2,
            "counts": {"ACKED": before_total},
        },
        "command_status_after": {
            "order_commands_allowed": False,
            "order_command_count": 2,
            "counts": {"ACKED": before_total + command_delta},
        },
        "consumer_status": {
            "status": "WARN",
            "consumer_enabled": False,
            "worker_enabled": False,
            "total_count": 3,
            "applied_count": 3,
            "pending_count": 0,
            "processing_count": 0,
            "dead_letter_count": dead_letter_count,
            "stale_processing_count": 0,
            "missing_inbox_count": 0,
            "applied_without_result_count": 0,
        },
        "inbox": {"read_only": True, "items": []},
        "dashboard_snapshot": {
            "live_sim_lifecycle_consumer": {"total_count": 3}
        },
    }


def test_ops_lifecycle_consumer_allows_disabled_preparation_as_warning() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "WARN"
    assert verdict["failures"] == []
    assert "LIFECYCLE_CONSUMER_DISABLED_PREPARATION" in verdict["warnings"]
    assert verdict["order_command_count_delta"] == 0


def test_ops_lifecycle_consumer_fails_on_dead_letter_or_command_delta() -> None:
    verdict = evaluate_report(_report(dead_letter_count=1, command_delta=1))

    assert verdict["status"] == "FAIL"
    assert "LIFECYCLE_DEAD_LETTER_COUNT" in verdict["failures"]
    assert "COMMAND_COUNT_CHANGED_DURING_CHECK" in verdict["failures"]
