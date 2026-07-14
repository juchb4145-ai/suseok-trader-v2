from __future__ import annotations

from tools.ops_runtime_execution_lock_check import evaluate_report


def test_runtime_execution_lock_ops_check_passes_empty_status() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["block_next_pr"] is False


def test_runtime_execution_lock_ops_check_passes_active_fenced_owner() -> None:
    report = _report()
    active = {
        "lock_name": "evaluation_pipeline",
        "owner_id": "owner-one",
        "process_id": 100,
        "thread_id": 200,
        "fencing_token": 7,
        "heartbeat_at": "2026-07-10T00:00:00Z",
        "owner_alive": True,
        "state": "ACTIVE",
    }
    report["lock_status"]["data"].update(
        {"lock_count": 1, "active_count": 1, "locks": [active]}
    )
    report["dashboard_snapshot"]["data"]["runtime_execution_locks"].update(
        {"lock_count": 1, "active_count": 1, "locks": [active]}
    )

    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["active_count"] == 1


def test_runtime_execution_lock_ops_check_fails_stale_lock() -> None:
    report = _report()
    report["lock_status"]["data"].update(
        {"status": "FAIL", "lock_count": 1, "stale_expired_count": 1}
    )
    report["dashboard_snapshot"]["data"]["runtime_execution_locks"][
        "lock_count"
    ] = 1

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "RUNTIME_EXECUTION_LOCK_STALE_EXPIRED" in verdict["failures"]
    assert verdict["block_next_pr"] is True


def _report() -> dict:
    status = {
        "status": "PASS",
        "lock_count": 0,
        "active_count": 0,
        "expired_owner_alive_count": 0,
        "stale_expired_count": 0,
        "locks": [],
        "read_only": True,
        "no_trading_side_effects": True,
    }
    return {
        "lock_status": {"ok": True, "data": dict(status)},
        "dashboard_snapshot": {
            "ok": True,
            "data": {"runtime_execution_locks": dict(status)},
        },
    }
