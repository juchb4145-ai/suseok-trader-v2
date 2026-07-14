from __future__ import annotations

from tools.ops_order_broker_boundary_check import evaluate_report


def test_order_broker_boundary_ops_check_passes_clean_contract() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["raw_status"] == "PASS"
    assert verdict["effective_status"] == "PASS"
    assert verdict["fast_0_status"] == "CLEAR"
    assert verdict["block_next_pr"] is False
    assert verdict["durable_pre_ack_count"] == 2


def test_order_broker_boundary_ops_check_warns_for_historical_unconfirmed() -> None:
    report = _report()
    report["boundary_status"]["data"].update(
        {
            "status": "WARN",
            "boundary_count": 3,
            "unconfirmed_count": 3,
            "block_new_order_routing": True,
            "state_counts": {
                "CLAIMED": 0,
                "GATEWAY_STARTED": 0,
                "PRE_ACK_RECORDED": 0,
                "BROKER_ACCEPTED": 0,
                "CHEJAN_CONFIRMED": 0,
                "UNCONFIRMED": 3,
            },
            "effective_state_counts": {
                "CLAIMED": 0,
                "GATEWAY_STARTED": 0,
                "PRE_ACK_RECORDED": 0,
                "BROKER_ACCEPTED": 0,
                "CHEJAN_CONFIRMED": 0,
                "UNCONFIRMED": 0,
                "RESOLVED_BROKER_NOT_REACHED": 3,
            },
            "effective_resolution_count": 3,
            "resolution_maintenance_fence_active_count": 3,
            "resolution_maintenance_fence_active": True,
            "effective_block_new_order_routing": True,
        }
    )
    report["dashboard_snapshot"]["data"]["order_broker_boundaries"].update(
        {"status": "WARN", "unconfirmed_count": 3, "boundary_count": 3}
    )

    verdict = evaluate_report(report)
    strict_verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "WARN"
    assert verdict["block_next_pr"] is False
    assert verdict["block_new_order_routing"] is True
    assert verdict["effective_block_new_order_routing"] is True
    assert verdict["raw_unconfirmed_count"] == 3
    assert verdict["effective_unconfirmed_count"] == 0
    assert verdict["fast_0_status"] == "CLEAR"
    assert strict_verdict["status"] == "WARN"
    assert "FAST_0_EFFECTIVE_ORDER_BOUNDARY_NOT_CLEAR" not in strict_verdict[
        "failures"
    ]


def test_order_broker_boundary_ops_check_requires_effective_clear() -> None:
    report = _report()
    report["boundary_status"]["data"].update(
        {
            "status": "WARN",
            "unconfirmed_count": 3,
            "block_new_order_routing": True,
            "effective_status": "WARN",
            "effective_unconfirmed_count": 3,
            "effective_block_new_order_routing": True,
            "fast_0_status": "BLOCKED",
        }
    )
    report["dashboard_snapshot"]["data"]["order_broker_boundaries"].update(
        {
            "status": "WARN",
            "unconfirmed_count": 3,
            "effective_status": "WARN",
            "effective_unconfirmed_count": 3,
            "fast_0_status": "BLOCKED",
        }
    )

    verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "FAIL"
    assert verdict["fast_0_status"] == "BLOCKED"
    assert verdict["effective_unconfirmed_count"] == 3
    assert "FAST_0_EFFECTIVE_ORDER_BOUNDARY_NOT_CLEAR" in verdict["failures"]


def test_order_broker_boundary_ops_check_falls_back_to_raw_contract() -> None:
    report = _report()
    status = report["boundary_status"]["data"]
    dashboard_status = report["dashboard_snapshot"]["data"][
        "order_broker_boundaries"
    ]
    for payload in (status, dashboard_status):
        for key in (
            "effective_status",
            "effective_unconfirmed_count",
            "effective_resolution_count",
            "invalidated_resolution_count",
            "effective_block_new_order_routing",
            "fast_0_status",
        ):
            payload.pop(key, None)
    status.update(
        {
            "status": "WARN",
            "unconfirmed_count": 2,
            "block_new_order_routing": True,
        }
    )
    dashboard_status.update({"status": "WARN", "unconfirmed_count": 2})

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["effective_status"] == "WARN"
    assert verdict["effective_unconfirmed_count"] == 2
    assert verdict["effective_block_new_order_routing"] is True
    assert verdict["fast_0_status"] == "BLOCKED"


def test_order_broker_boundary_strict_mode_rejects_legacy_effective_contract() -> None:
    report = _report()
    status = report["boundary_status"]["data"]
    dashboard_status = report["dashboard_snapshot"]["data"][
        "order_broker_boundaries"
    ]
    for payload in (status, dashboard_status):
        for key in (
            "effective_status",
            "effective_unconfirmed_count",
            "effective_resolution_count",
            "invalidated_resolution_count",
            "effective_block_new_order_routing",
            "fast_0_status",
        ):
            payload.pop(key, None)

    verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "FAIL"
    assert verdict["effective_contract_present"] is False
    assert "FAST_0_EFFECTIVE_CONTRACT_INVALID" in verdict["failures"]


def test_order_broker_boundary_ops_warns_for_invalidated_resolution() -> None:
    report = _report()
    for payload in (
        report["boundary_status"]["data"],
        report["dashboard_snapshot"]["data"]["order_broker_boundaries"],
    ):
        payload.update(
            {
                "effective_status": "WARN",
                "effective_block_new_order_routing": True,
                "qualification_block_new_order_routing": True,
                "invalidated_resolution_count": 1,
                "fast_0_status": "BLOCKED",
            }
        )

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["fast_0_status"] == "BLOCKED"
    assert "ORDER_BOUNDARY_RESOLUTION_INVALIDATED" in verdict["warnings"]


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


def test_order_broker_boundary_ops_check_fails_same_total_state_change() -> None:
    report = _report()
    report["command_status_after"]["data"]["counts"] = {
        "ACKED": 4,
        "CLAIMED": 1,
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert verdict["command_count_delta"] == 0
    assert "COMMAND_STATE_COUNTS_CHANGED_DURING_CHECK" in verdict["failures"]


def test_order_broker_boundary_strict_invalid_contract_is_fail_closed() -> None:
    report = _report()
    report["boundary_status"]["data"].update(
        {
            "effective_unconfirmed_count": "0",
            "invalidated_resolution_count": "0",
            "fast_0_status": "CLEAR",
        }
    )

    verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "FAIL"
    assert verdict["fast_0_status"] == "BLOCKED"
    assert "FAST_0_EFFECTIVE_CONTRACT_INVALID" in verdict["failures"]


def test_order_broker_boundary_strict_rejects_false_schema_readiness() -> None:
    report = _report()
    report["boundary_status"]["data"].update(
        {
            "resolution_schema_ready": False,
            "resolution_table_exists": False,
            "resolution_required_indexes_present": False,
            "resolution_append_only_triggers_present": False,
        }
    )

    verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "FAIL"
    assert verdict["fast_0_status"] == "BLOCKED"
    assert "FAST_0_EFFECTIVE_CONTRACT_INVALID" in verdict["failures"]


def test_order_broker_boundary_strict_rejects_truncated_state_counts() -> None:
    report = _report()
    report["boundary_status"]["data"]["effective_state_counts"] = {}

    verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "FAIL"
    assert verdict["fast_0_status"] == "BLOCKED"
    assert "FAST_0_EFFECTIVE_CONTRACT_INVALID" in verdict["failures"]


def test_order_broker_boundary_strict_rejects_active_order_commands() -> None:
    report = _report()
    report["boundary_status"]["data"].update(
        {
            "active_order_command_count": 1,
            "qualification_block_new_order_routing": True,
            "fast_0_status": "BLOCKED",
        }
    )

    verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "FAIL"
    assert verdict["fast_0_status"] == "BLOCKED"
    assert "FAST_0_ACTIVE_ORDER_COMMANDS_PRESENT" in verdict["failures"]


def test_order_broker_boundary_ops_rejects_unknown_command_status_key() -> None:
    report = _report()
    for key in ("command_status", "command_status_after"):
        report[key]["data"]["counts"] = {"BROKER_CALLING": 5}

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "COMMAND_STATUS_COUNTS_INVALID" in verdict["failures"]


def test_order_broker_boundary_ops_requires_explicit_observe_profile_and_flags() -> None:
    report = _report()
    report["core_status"]["data"].pop("profile")
    report["core_status"]["data"].pop("live_sim_allowed")

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "CORE_PROFILE_NOT_OBSERVE" in verdict["failures"]
    assert "LIVE_SIM_ALLOWED" in verdict["failures"]


def test_order_broker_boundary_ops_requires_expected_database_identity() -> None:
    report = _report()
    report["core_status"]["data"]["database_path"] = (
        "C:/safe/different.sqlite3"
    )

    verdict = evaluate_report(report, require_effective_clear=True)

    assert verdict["status"] == "FAIL"
    assert "CORE_DATABASE_PATH_MISMATCH" in verdict["failures"]


def test_order_broker_boundary_ops_check_requires_zero_modify_order_rows() -> None:
    report = _report()
    report["command_status"]["data"]["command_type_counts"] = {
        "modify_order": 1
    }
    report["command_status_after"]["data"]["command_type_counts"] = {
        "modify_order": 1
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert verdict["modify_order_count"] == 1
    assert "MODIFY_ORDER_COMMAND_PRESENT" in verdict["failures"]


def _report() -> dict:
    status = {
        "status": "PASS",
        "table_exists": True,
        "required_indexes_present": True,
        "boundary_count": 2,
        "active_order_command_count": 0,
        "unknown_command_status_count": 0,
        "durable_pre_ack_count": 2,
        "missing_boundary_count": 0,
        "durable_pre_ack_gap_count": 0,
        "duplicate_idempotency_count": 0,
        "command_state_mismatch_count": 0,
        "unknown_state_count": 0,
        "orphan_boundary_count": 0,
        "unexpected_boundary_count": 0,
        "linked_command_type_invalid_count": 0,
        "linked_command_type_mismatch_count": 0,
        "invalid_command_type_count": 0,
        "invalid_scope_count": 0,
        "invalid_resolution_chain_count": 0,
        "unconfirmed_count": 0,
        "block_new_order_routing": False,
        "effective_status": "PASS",
        "state_counts": {
            "CLAIMED": 0,
            "GATEWAY_STARTED": 0,
            "PRE_ACK_RECORDED": 2,
            "BROKER_ACCEPTED": 0,
            "CHEJAN_CONFIRMED": 0,
            "UNCONFIRMED": 0,
        },
        "effective_state_counts": {
            "CLAIMED": 0,
            "GATEWAY_STARTED": 0,
            "PRE_ACK_RECORDED": 2,
            "BROKER_ACCEPTED": 0,
            "CHEJAN_CONFIRMED": 0,
            "UNCONFIRMED": 0,
            "RESOLVED_BROKER_NOT_REACHED": 0,
        },
        "effective_unconfirmed_count": 0,
        "effective_resolution_count": 0,
        "invalidated_resolution_count": 0,
        "resolution_maintenance_fence_active_count": 0,
        "resolution_maintenance_fence_active": False,
        "qualification_block_new_order_routing": False,
        "effective_block_new_order_routing": False,
        "fast_0_status": "CLEAR",
        "resolution_schema_ready": True,
        "resolution_source_schema_ready": True,
        "resolution_table_exists": True,
        "resolution_required_indexes_present": True,
        "resolution_append_only_triggers_present": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    command_status = {
        "counts": {"ACKED": 5},
        "command_type_counts": {"request_tr": 5},
        "order_command_count": 5,
        "order_commands_allowed": False,
    }
    return {
        "expected_database_path": "C:/safe/operating.sqlite3",
        "core_status": {
            "ok": True,
            "data": {
                "profile": "OBSERVE",
                "mode": "OBSERVE",
                "database_path": "C:/safe/operating.sqlite3",
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
