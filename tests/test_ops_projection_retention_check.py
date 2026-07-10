from __future__ import annotations

from tools.ops_projection_retention_check import evaluate_report


def test_projection_retention_ops_check_passes_ready_contract() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["block_next_pr"] is False
    assert verdict["retention_eligible_event_count"] == 3


def test_projection_retention_ops_check_warns_for_safe_disabled_and_blocked() -> None:
    report = _report()
    report["watermark_status"]["data"]["unresolved_error_count"] = 1
    report["retention_status"]["data"].update(
        {
            "status": "WARN",
            "enabled": False,
            "apply_ready": False,
            "age_eligible_event_count": 5,
            "candidate_event_count": 3,
            "projection_blocked_event_count": 2,
            "projection_retention_gate_pass": False,
        }
    )
    report["dashboard_snapshot"]["data"]["projection_retention"].update(
        {"projection_blocked_event_count": 2}
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["block_next_pr"] is False
    assert "EVENT_RETENTION_DISABLED_SAFE_DEFAULT" in verdict["warnings"]
    assert "PROJECTION_RETENTION_EVENTS_BLOCKED" in verdict["warnings"]


def test_projection_retention_ops_check_fails_command_delta_or_count_mismatch() -> None:
    report = _report()
    report["command_status_after"]["data"]["counts"] = {"ACKED": 2}
    report["retention_status"]["data"]["age_eligible_event_count"] = 10

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "COMMAND_COUNT_CHANGED_DURING_CHECK" in verdict["failures"]
    assert "RETENTION_COUNT_CONSERVATION_MISMATCH" in verdict["failures"]


def _report() -> dict:
    command_status = {
        "counts": {"ACKED": 1},
        "order_command_count": 1,
        "order_commands_allowed": False,
    }
    retention = {
        "status": "PASS",
        "enabled": True,
        "apply_ready": True,
        "age_eligible_event_count": 3,
        "candidate_event_count": 3,
        "projection_blocked_event_count": 0,
        "projection_retention_gate_pass": True,
        "counts_exact": True,
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
        "watermark_status": {
            "ok": True,
            "data": {"status": "PASS", "unresolved_error_count": 0},
        },
        "retention_status": {"ok": True, "data": dict(retention)},
        "retention_rca": {
            "ok": True,
            "data": {"read_only": True, "items": []},
        },
        "backfill_dry_run": {
            "ok": True,
            "data": {"dry_run": True, "applied_count": 0, "candidate_count": 0},
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "projection_watermarks": {"status": "PASS"},
                "projection_retention": dict(retention),
                "errors": {"projection_retention_rca": {}},
            },
        },
    }
