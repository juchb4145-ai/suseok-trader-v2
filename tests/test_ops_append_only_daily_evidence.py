from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
from services.runtime.append_only_readiness import REQUIRED_COMPONENTS
from tools.ops_append_only_daily_evidence import (
    evaluate_preflight,
    evaluate_report,
    render_markdown_summary,
    run_daily_evidence_report,
    validate_persistent_db_path,
)


def _wrapped(data):
    return {"ok": True, "status_code": 200, "data": data}


def _report(tmp_path: Path) -> dict:
    trade_date = "2026-07-13"
    db_path = tmp_path / "append-only-10day.sqlite3"
    db_path.touch()
    gateway = {
        "last_heartbeat_at": "2026-07-13T06:31:00Z",
        "last_event_received_at": "2026-07-13T06:31:00Z",
    }
    command_status = {
        "counts": {"ACKED": 7, "FAILED": 0},
        "order_command_count": 0,
    }
    component_statuses = {
        component: {
            "latest_trade_date": trade_date,
            "latest": {
                "passed": True,
                "run_date": trade_date,
                "run_id": f"{component}:{trade_date}",
                "status": "PASS" if component != "live_sim_lifecycle" else "IDLE",
                "reason_codes": [],
            },
        }
        for component in REQUIRED_COMPONENTS
    }
    readiness = {
        "status": "BLOCKED_EVIDENCE",
        "configuration": {"ready": True},
        "current_health": {"ready": True},
        "component_statuses": component_statuses,
        "consecutive_qualified_trading_day_count": 1,
        "required_trading_days": 10,
        "automatic_cutover_allowed": False,
        "flag_cleanup_allowed": False,
        "raw_append_enqueue_only_enabled": False,
        "request_path_removal_performed": False,
        "emergency_inline_fallback_retained": True,
    }
    return {
        "trade_date": trade_date,
        "expected_db_path": str(db_path),
        "core_url": "http://127.0.0.1:8040",
        "session_state": {
            "trade_date": trade_date,
            "core_url": "http://127.0.0.1:8040",
            "database_path": str(db_path),
            "command_count": 7,
            "failed_command_count": 0,
            "order_command_count": 0,
        },
        "core_status": _wrapped(
            {
                "profile": "OBSERVE",
                "mode": "OBSERVE",
                "live_sim_allowed": False,
                "live_real_allowed": False,
                "database_path": str(db_path),
            }
        ),
        "initial_readiness": _wrapped(readiness),
        "command_status_before": _wrapped(deepcopy(command_status)),
        "command_status_after": _wrapped(deepcopy(command_status)),
        "gateway_status_before_settle": _wrapped(deepcopy(gateway)),
        "gateway_status_after_settle": _wrapped(deepcopy(gateway)),
        "gateway_status_final": _wrapped(deepcopy(gateway)),
        "projection_outbox_drain": {
            "status": "COMPLETED",
            "remaining_pending_count": 0,
            "error_count": 0,
            "dead_letter_count": 0,
        },
        "lifecycle_run": _wrapped(
            {"status": "IDLE", "error_count": 0, "dead_letter_count": 0}
        ),
        "market_context_rebuild": _wrapped(
            {"status": "APPLIED", "no_trading_side_effects": True}
        ),
        "reconcile_runs": {
            component: _wrapped({"status": "PASS"})
            for component in REQUIRED_COMPONENTS
            if component != "live_sim_lifecycle"
        },
        "final_readiness": _wrapped(readiness),
        "dashboard": _wrapped({"append_only_readiness": readiness}),
        "projection_outbox": _wrapped({"counts": {"PENDING": 0}}),
        "preflight": {"status": "PASS", "failures": []},
    }


def test_daily_evidence_report_passes_only_when_all_components_qualify(tmp_path) -> None:
    report = _report(tmp_path)

    assert evaluate_preflight(report) == []
    verdict = evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert verdict["qualified_today"] is True
    assert verdict["consecutive_qualified_trading_day_count"] == 1
    assert verdict["command_count_delta"] == 0
    assert verdict["order_command_count_delta"] == 0
    assert all(item["passed"] for item in verdict["component_results"].values())
    report["verdict"] = verdict
    assert "command/order-command delta: `0/0`" in render_markdown_summary(report)


def test_daily_evidence_preflight_rejects_active_ingest_and_wrong_db(tmp_path) -> None:
    report = _report(tmp_path)
    report["core_status"]["data"]["database_path"] = str(tmp_path / "wrong.sqlite3")
    report["gateway_status_after_settle"]["data"]["last_heartbeat_at"] = (
        "2026-07-13T06:31:01Z"
    )

    failures = evaluate_preflight(report)

    assert "CORE_DATABASE_PATH_MISMATCH" in failures
    assert "GATEWAY_INGEST_NOT_STOPPED" in failures


def test_daily_evidence_fails_when_one_component_or_command_delta_is_missing(
    tmp_path,
) -> None:
    report = _report(tmp_path)
    market_scan = report["final_readiness"]["data"]["component_statuses"][
        "market_scan"
    ]
    market_scan["latest"]["passed"] = False
    market_scan["latest"]["reason_codes"] = ["OUTBOX_PENDING_COUNT_NONZERO"]
    report["command_status_after"]["data"]["counts"]["ACKED"] = 8
    report["command_status_after"]["data"]["counts"]["FAILED"] = 1

    verdict = evaluate_report(report)

    assert verdict["status"] == "FAIL"
    assert "MARKET_SCAN_DAILY_EVIDENCE_NOT_QUALIFIED" in verdict["failures"]
    assert "COMMAND_COUNT_CHANGED_DURING_DAILY_CLOSE" in verdict["failures"]
    assert "FAILED_COMMAND_COUNT_CHANGED_DURING_SESSION" in verdict["failures"]


def test_persistent_evidence_db_rejects_temp_and_missing_paths(
    tmp_path,
    monkeypatch,
) -> None:
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    temp_db = temp_root / "daily.sqlite3"
    temp_db.touch()
    monkeypatch.setattr("tempfile.gettempdir", lambda: str(temp_root))

    with pytest.raises(ValueError, match="cannot be stored under TEMP"):
        validate_persistent_db_path(temp_db)
    with pytest.raises(ValueError, match="does not exist"):
        validate_persistent_db_path(tmp_path / "missing.sqlite3")


def test_daily_evidence_rebuilds_context_before_reconcile_and_extends_scan_timeout(
    tmp_path,
    monkeypatch,
) -> None:
    baseline = _report(tmp_path)
    calls: list[tuple[str, float, str]] = []

    def fake_fetch(base_url, path, token, timeout_sec, *, method="GET"):
        del base_url, token
        calls.append((path, timeout_sec, method))
        route = path.split("?", 1)[0]
        if route == "/api/status":
            return baseline["core_status"]
        if route == "/api/operator/append-only-readiness/status":
            return baseline["final_readiness"]
        if route == "/api/gateway/commands/status":
            return baseline["command_status_after"]
        if route == "/api/gateway/status":
            return baseline["gateway_status_final"]
        if route == "/api/operator/projection-outbox/run-once":
            return _wrapped(
                {
                    "status": "COMPLETED",
                    "remaining_pending_count": 0,
                    "error_count": 0,
                    "dead_letter_count": 0,
                }
            )
        if route == "/api/operator/live-sim/lifecycle-consumer/run-once":
            return baseline["lifecycle_run"]
        if route == "/api/operator/market-context/rebuild":
            return baseline["market_context_rebuild"]
        if route.endswith("projection-reconcile/run-once"):
            return _wrapped({"status": "PASS"})
        if route == "/api/dashboard/snapshot":
            return baseline["dashboard"]
        if route == "/api/operator/projection-outbox/status":
            return baseline["projection_outbox"]
        raise AssertionError(f"unexpected route: {route}")

    monkeypatch.setattr(
        "tools.ops_append_only_daily_evidence._fetch",
        fake_fetch,
    )

    report = run_daily_evidence_report(
        core_url=baseline["core_url"],
        token="test-token",
        expected_db_path=baseline["expected_db_path"],
        session_state_path=str(tmp_path / ".session.json"),
        session_state=baseline["session_state"],
        trade_date=baseline["trade_date"],
        settle_sec=0,
        drain_limit=500,
        drain_max_batches=1,
        reconcile_limit=5000,
        timeout_sec=30.0,
        out_dir=tmp_path / "reports",
    )

    context_index = next(
        index
        for index, call in enumerate(calls)
        if call[0].startswith("/api/operator/market-context/rebuild")
    )
    reconcile_indexes = [
        index
        for index, call in enumerate(calls)
        if "projection-reconcile/run-once" in call[0]
    ]
    scan_call = next(
        call
        for call in calls
        if call[0].startswith(
            "/api/operator/market-scan-projection-reconcile/run-once"
        )
    )

    assert report["verdict"]["status"] == "PASS"
    assert context_index < min(reconcile_indexes)
    assert scan_call[1] == 120.0
    assert scan_call[2] == "POST"
