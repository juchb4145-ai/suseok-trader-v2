import pytest
from tools.ops_incremental_evaluation_queue_check import evaluate_report


def _report(
    *,
    require_effective_clear: bool = False,
    raw_dead_letter_count: int = 0,
    effective_dead_letter_count: int = 0,
    active_unresolved_count: int = 0,
    historical_pending_count: int = 0,
    historical_disposed_count: int = 0,
    manual_review_count: int = 0,
    invalid_disposition_count: int = 0,
    include_effective_contract: bool = True,
):
    raw_status = "FAIL" if raw_dead_letter_count else "PASS"
    effective_status = "FAIL" if effective_dead_letter_count else "PASS"
    status = raw_status
    reason_codes = (
        ["INCREMENTAL_QUEUE_DEAD_LETTER_PRESENT"]
        if raw_dead_letter_count
        else []
    )
    effective_fields = {
        "raw_dead_letter_count": raw_dead_letter_count,
        "effective_dead_letter_count": effective_dead_letter_count,
        "active_unresolved_dead_letter_count": active_unresolved_count,
        "historical_pending_disposition_count": historical_pending_count,
        "historical_disposed_dead_letter_count": historical_disposed_count,
        "manual_review_dead_letter_count": manual_review_count,
        "invalid_disposition_count": invalid_disposition_count,
        "raw_status": raw_status,
        "effective_status": effective_status,
        "fast_0_status": "BLOCKED" if effective_dead_letter_count else "CLEAR",
    }
    queue_status = {
        "status": status,
        "queued_count": 0,
        "retry_exhausted_count": 0,
        "dead_letter_count": raw_dead_letter_count,
        "stale_queue_count": 0,
        "reason_codes": reason_codes,
    }
    if include_effective_contract:
        queue_status.update(effective_fields)
    return {
        "requested_contract": {
            "require_effective_clear": require_effective_clear,
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
        "queue_status_before": dict(queue_status),
        "queue_status_after": dict(queue_status),
        "dead_letters_before": {"count": raw_dead_letter_count, "items": []},
        "dead_letters_after": {"count": raw_dead_letter_count, "items": []},
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


def test_ops_incremental_queue_default_contract_fails_on_raw_dead_letter() -> None:
    verdict = evaluate_report(
        _report(
            raw_dead_letter_count=1,
            effective_dead_letter_count=1,
            historical_pending_count=1,
        )
    )

    assert verdict["status"] == "FAIL"
    assert "INCREMENTAL_QUEUE_STATUS_FAIL" in verdict["failures"]
    assert "INCREMENTAL_DEAD_LETTER_PRESENT" in verdict["failures"]


def test_ops_incremental_queue_effective_clear_preserves_historical_raw_rows() -> None:
    verdict = evaluate_report(
        _report(
            require_effective_clear=True,
            raw_dead_letter_count=38,
            historical_disposed_count=38,
        )
    )

    assert verdict["status"] == "WARN"
    assert verdict["failures"] == []
    assert verdict["raw_dead_letter_after"] == 38
    assert verdict["effective_dead_letter_after"] == 0
    assert verdict["historical_disposed_after"] == 38
    assert verdict["fast_0_status"] == "CLEAR"
    assert "INCREMENTAL_HISTORICAL_DEAD_LETTER_PRESERVED" in verdict["warnings"]


@pytest.mark.parametrize(
    ("overrides", "failure"),
    [
        (
            {"active_unresolved_count": 1},
            "INCREMENTAL_ACTIVE_DEAD_LETTER_PRESENT",
        ),
        (
            {"historical_pending_count": 1},
            "INCREMENTAL_HISTORICAL_DISPOSITION_PENDING",
        ),
        (
            {"manual_review_count": 1},
            "INCREMENTAL_DEAD_LETTER_MANUAL_REVIEW_REQUIRED",
        ),
        (
            {"invalid_disposition_count": 1},
            "INCREMENTAL_INVALID_DISPOSITION_PRESENT",
        ),
    ],
)
def test_ops_incremental_queue_effective_clear_rejects_unresolved_buckets(
    overrides,
    failure,
) -> None:
    report = _report(
        require_effective_clear=True,
        raw_dead_letter_count=1,
        effective_dead_letter_count=1,
        **overrides,
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert failure in verdict["failures"]


def test_ops_incremental_queue_effective_clear_fails_closed_without_contract() -> None:
    verdict = evaluate_report(
        _report(
            require_effective_clear=True,
            include_effective_contract=False,
        )
    )

    assert verdict["status"] == "FAIL"
    assert "INCREMENTAL_EFFECTIVE_STATUS_CONTRACT_MISSING" in verdict["failures"]
    assert (
        "INCREMENTAL_EFFECTIVE_STATUS_BEFORE_CONTRACT_MISSING"
        in verdict["failures"]
    )


def test_ops_incremental_queue_rejects_stringified_effective_counts() -> None:
    report = _report(require_effective_clear=True)
    report["queue_status_before"]["effective_dead_letter_count"] = "0"
    report["queue_status_after"]["effective_dead_letter_count"] = "0"

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "INCREMENTAL_EFFECTIVE_STATUS_CONTRACT_MISSING" in verdict["failures"]
