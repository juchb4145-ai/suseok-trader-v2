from tools.ops_append_only_readiness_check import evaluate_report


def test_ops_append_only_readiness_warns_while_safely_blocked() -> None:
    verdict = evaluate_report(_report(status="BLOCKED_EVIDENCE", days=3))

    assert verdict["status"] == "WARN"
    assert verdict["block_flag_cleanup"] is True
    assert verdict["command_count_delta"] == 0


def test_ops_append_only_readiness_passes_operator_review_threshold() -> None:
    report = _report(status="READY_FOR_OPERATOR_REVIEW", days=10)
    report["expect_ready"] = True

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["consecutive_qualified_trading_day_count"] == 10
    assert verdict["block_flag_cleanup"] is False


def test_ops_append_only_readiness_rejects_automatic_cleanup() -> None:
    report = _report(status="READY_FOR_OPERATOR_REVIEW", days=10)
    report["readiness"]["flag_cleanup_allowed"] = True

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "FLAG_CLEANUP_ALLOWED_WITHOUT_OPERATOR_REVIEW" in verdict["failures"]


def _report(*, status: str, days: int) -> dict:
    readiness = {
        "status": status,
        "required_trading_days": 10,
        "consecutive_qualified_trading_day_count": days,
        "read_only": True,
        "automatic_cutover_allowed": False,
        "flag_cleanup_allowed": False,
        "request_path_removal_performed": False,
        "emergency_inline_fallback_retained": True,
    }
    return {
        "expect_ready": False,
        "core_status": {
            "mode": "OBSERVE",
            "live_sim_allowed": False,
            "live_real_allowed": False,
        },
        "command_status_before": {
            "counts": {"ACKED": 2},
            "order_command_count": 0,
        },
        "readiness": readiness,
        "dashboard": {"append_only_readiness": dict(readiness)},
        "command_status_after": {
            "counts": {"ACKED": 2},
            "order_command_count": 0,
        },
    }
