from __future__ import annotations

from tools.ops_order_broker_boundary_check import evaluate_report


def test_order_broker_boundary_ops_check_passes_clean_contract() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["block_next_pr"] is False
    assert verdict["durable_pre_ack_count"] == 2


def test_order_broker_boundary_ops_check_warns_for_historical_unconfirmed() -> None:
    report = _report()
    report["boundary_status"]["data"].update(
        {
            "status": "WARN",
            "unconfirmed_count": 3,
            "block_new_order_routing": True,
        }
    )
    report["dashboard_snapshot"]["data"]["order_broker_boundaries"].update(
        {"status": "WARN", "unconfirmed_count": 3}
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["block_next_pr"] is False
    assert verdict["block_new_order_routing"] is True


def test_order_broker_boundary_ops_check_fails_durable_gap() -> None:
    report = _report()
    report["boundary_status"]["data"].update(
        {"status": "FAIL", "durable_pre_ack_gap_count": 1}
    )
    report["dashboard_snapshot"]["data"]["order_broker_boundaries"][
        "status"
    ] = "FAIL"

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "ORDER_BOUNDARY_DURABLE_PRE_ACK_GAP_COUNT" in verdict["failures"]


def test_order_broker_boundary_ops_check_fails_command_delta() -> None:
    report = _report()
    report["command_status_after"]["data"].update(
        {"counts": {"ACKED": 6}, "order_command_count": 6}
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "COMMAND_COUNT_CHANGED_DURING_CHECK" in verdict["failures"]
    assert "ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK" in verdict["failures"]


def _report() -> dict:
    status = {
        "status": "PASS",
        "table_exists": True,
        "required_indexes_present": True,
        "boundary_count": 2,
        "durable_pre_ack_count": 2,
        "missing_boundary_count": 0,
        "durable_pre_ack_gap_count": 0,
        "duplicate_idempotency_count": 0,
        "command_state_mismatch_count": 0,
        "unconfirmed_count": 0,
        "block_new_order_routing": False,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    command_status = {
        "counts": {"ACKED": 5},
        "order_command_count": 5,
        "order_commands_allowed": False,
    }
    return {
        "core_status": {
            "ok": True,
            "data": {
                "mode": "OBSERVE",
                "live_sim_allowed": False,
                "live_real_allowed": False,
            },
        },
        "command_status": {"ok": True, "data": dict(command_status)},
        "command_status_after": {"ok": True, "data": dict(command_status)},
        "boundary_status": {"ok": True, "data": dict(status)},
        "boundary_rows": {
            "ok": True,
            "data": {"items": [], "count": 0, "read_only": True},
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {"order_broker_boundaries": dict(status)},
        },
    }
