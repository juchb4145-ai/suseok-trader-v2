from __future__ import annotations

from tools.ops_market_reference_projection_check import evaluate_report


def test_ops_market_reference_projection_check_passes_ready_report() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["block_pr14"] is False


def test_ops_market_reference_projection_check_fails_missing_membership() -> None:
    report = _report()
    report["latest_reconcile"]["data"]["latest_run"]["status"] = "FAIL"
    report["latest_reconcile"]["data"]["latest_run"]["missing_membership_count"] = 1
    report["market_reference_status"]["data"]["missing_membership_count"] = 1

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "MARKET_REFERENCE_MEMBERSHIP_MISSING" in verdict["failures"]
    assert verdict["block_pr14"] is True


def test_ops_market_reference_projection_check_blocks_pr14_without_worker_evidence() -> None:
    report = _report()
    report["market_reference_status"]["data"]["latest_outbox_job"] = None

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert "MARKET_REFERENCE_WORKER_EVIDENCE_MISSING" in verdict["warnings"]
    assert verdict["block_pr14"] is True


def test_ops_market_reference_projection_check_passes_limited_cutover_evidence() -> None:
    report = _report()
    report["expect_effective_skip"] = True
    routing = report["routing_status"]["data"]
    routing.update(
        {
            "status": "PASS",
            "cutover_enabled": True,
            "global_kill_switch": False,
            "worker_apply_enabled": True,
            "skip_budget_limit": 1,
            "skip_budget_used_current_minute": 1,
            "effective_skip_inline_count": 1,
            "rollback_required": False,
            "effective_skip_health": {
                "pending_worker_count": 0,
                "worker_error_count": 0,
                "artifact_missing_count": 0,
            },
            "latest_decision": {
                "event_id": "evt_ref_latest",
                "effective_skip_inline": True,
                "evidence": {"skip_budget_limit": 1, "skip_budget_used": 1},
            },
        }
    )
    report["market_reference_status"]["data"][
        "latest_market_symbols_event_id"
    ] = "evt_ref_latest"
    report["market_reference_status"]["data"]["effective_skip_inline_count"] = 1
    report["market_reference_status"]["data"]["latest_outbox_job"]["metadata"][
        "last_worker_evidence"
    ]["apply_result"] = "APPLIED_BY_WORKER"
    report["dashboard_snapshot"]["data"]["market_reference"][
        "effective_skip_inline_count"
    ] = 1
    report["dashboard_snapshot"]["data"]["gateway"] = {
        "realtime_exchange": "NXT",
        "kiwoom_logged_in": True,
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["controller_status"] == "PASS"
    assert verdict["effective_skip_inline_count"] == 1
    assert verdict["block_next_pr"] is False


def test_ops_market_reference_projection_check_fails_pending_effective_skip() -> None:
    report = _report()
    report["expect_effective_skip"] = True
    routing = report["routing_status"]["data"]
    routing.update(
        {
            "status": "FAIL",
            "cutover_enabled": True,
            "global_kill_switch": False,
            "worker_apply_enabled": True,
            "skip_budget_limit": 1,
            "effective_skip_inline_count": 1,
            "rollback_required": True,
            "effective_skip_health": {
                "pending_worker_count": 1,
                "worker_error_count": 0,
                "artifact_missing_count": 1,
            },
            "latest_decision": {
                "event_id": "evt_ref_latest",
                "effective_skip_inline": True,
                "evidence": {"skip_budget_limit": 1, "skip_budget_used": 1},
            },
        }
    )
    report["market_reference_status"]["data"][
        "latest_market_symbols_event_id"
    ] = "evt_ref_latest"
    report["dashboard_snapshot"]["data"]["market_reference"][
        "effective_skip_inline_count"
    ] = 1
    report["dashboard_snapshot"]["data"]["gateway"] = {
        "realtime_exchange": "NXT",
        "kiwoom_logged_in": True,
    }

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "MARKET_REFERENCE_EFFECTIVE_SKIP_PENDING_WORKER" in verdict["failures"]
    assert verdict["block_next_pr"] is True


def _report() -> dict:
    latest_run = {
        "status": "PASS",
        "append_only_ready": True,
        "stored_membership_count": 120,
        "missing_membership_count": 0,
        "outbox_pending_count": 0,
        "outbox_error_count": 0,
        "outbox_dead_letter_count": 0,
        "reason_codes": [],
    }
    return {
        "reconcile_run": {"ok": True, "data": latest_run},
        "latest_reconcile": {"ok": True, "data": {"latest_run": dict(latest_run)}},
        "routing_status": {
            "ok": True,
            "data": {
                "status": "WARN",
                "dry_run_enabled": True,
                "cutover_enabled": False,
                "global_kill_switch": True,
                "worker_apply_enabled": True,
                "skip_budget_limit": 0,
                "rollback_required": False,
                "effective_skip_inline_count": 0,
                "would_skip_inline_count": 1,
                "effective_skip_health": {},
            },
        },
        "market_reference_status": {
            "ok": True,
            "data": {
                "membership_count": 120,
                "missing_membership_count": 0,
                "effective_skip_inline_count": 0,
                "latest_outbox_job": {
                    "status": "APPLIED",
                    "metadata": {
                        "last_worker_evidence": {
                            "apply_mode": "MARKET_REFERENCE_APPLY",
                            "apply_result": "APPLIED_BY_VERIFY",
                            "no_trading_side_effects": True,
                        }
                    },
                },
                "latest_market_symbols_event_id": "evt_ref_latest",
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {
                "by_projection_name": {
                    "market_reference": {
                        "error_count": 0,
                        "dead_letter_count": 0,
                    }
                }
            },
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "gateway": {
                    "realtime_exchange": "NXT",
                    "kiwoom_logged_in": True,
                },
                "market_reference": {
                    "effective_skip_inline_count": 0,
                }
            },
        },
    }
