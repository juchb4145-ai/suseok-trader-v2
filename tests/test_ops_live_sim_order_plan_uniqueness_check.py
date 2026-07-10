from __future__ import annotations

from tools.ops_live_sim_order_plan_uniqueness_check import evaluate_report


def test_order_plan_uniqueness_ops_check_passes_valid_contract() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["block_next_pr"] is False
    assert verdict["order_plan_intent_count"] == 7


def test_order_plan_uniqueness_ops_check_fails_duplicate_or_missing_backfill() -> None:
    report = _report()
    report["uniqueness_status"]["data"].update(
        {
            "status": "FAIL",
            "duplicate_group_count": 1,
            "missing_backfill_count": 2,
        }
    )
    report["dashboard_snapshot"]["data"]["live_sim_order_plan_uniqueness"][
        "status"
    ] = "FAIL"

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "ORDER_PLAN_DUPLICATE_GROUP_COUNT" in verdict["failures"]
    assert "ORDER_PLAN_MISSING_BACKFILL_COUNT" in verdict["failures"]


def test_order_plan_uniqueness_ops_check_fails_command_delta() -> None:
    report = _report()
    report["command_status_after"]["data"].update(
        {
            "counts": {"ACKED": 6},
            "order_command_count": 1,
        }
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "COMMAND_COUNT_CHANGED_DURING_CHECK" in verdict["failures"]
    assert "ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK" in verdict["failures"]


def _report() -> dict:
    status = {
        "status": "PASS",
        "column_exists": True,
        "unique_index_exists": True,
        "unique_index_is_unique": True,
        "unique_index_is_partial": True,
        "lookup_strategy": "DIRECT_ORDER_PLAN_ID_INDEX_LOOKUP",
        "intent_count": 7,
        "order_plan_intent_count": 7,
        "duplicate_group_count": 0,
        "mismatch_count": 0,
        "missing_backfill_count": 0,
        "invalid_evidence_order_plan_id_count": 0,
        "no_trading_side_effects": True,
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
        "command_status": {
            "ok": True,
            "data": {
                "counts": {"ACKED": 5},
                "order_command_count": 0,
                "order_commands_allowed": False,
            },
        },
        "command_status_after": {
            "ok": True,
            "data": {
                "counts": {"ACKED": 5},
                "order_command_count": 0,
                "order_commands_allowed": False,
            },
        },
        "uniqueness_status": {"ok": True, "data": dict(status)},
        "dashboard_snapshot": {
            "ok": True,
            "data": {"live_sim_order_plan_uniqueness": dict(status)},
        },
    }
