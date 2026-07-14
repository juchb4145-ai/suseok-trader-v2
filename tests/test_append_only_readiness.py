from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from services.config import Settings
from services.runtime.append_only_readiness import (
    REQUIRED_COMPONENTS,
    _projection_failure_reasons,
    build_append_only_readiness_status,
    evaluate_append_only_readiness,
)
from storage.sqlite import initialize_database


def test_append_only_readiness_empty_database_is_fail_closed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "append-only-empty.sqlite3")
    result = build_append_only_readiness_status(
        connection,
        settings=Settings(),
    )
    connection.close()

    assert result["status"] == "BLOCKED_CONFIG"
    assert result["consecutive_qualified_trading_day_count"] == 0
    assert result["automatic_cutover_allowed"] is False
    assert result["flag_cleanup_allowed"] is False
    assert result["request_path_removal_performed"] is False
    assert result["emergency_inline_fallback_retained"] is True


def test_append_only_readiness_missing_component_schema_is_reported(tmp_path) -> None:
    connection = initialize_database(tmp_path / "append-only-old-schema.sqlite3")
    connection.execute("DROP TABLE market_scan_projection_reconcile_runs")
    connection.commit()

    result = build_append_only_readiness_status(connection, settings=Settings())
    connection.close()

    assert result["schema_ready"] is False
    assert result["status"] == "BLOCKED_SCHEMA"
    assert result["schema_availability"]["market_scan"] is False
    assert "APPEND_ONLY_EVIDENCE_SCHEMA_INCOMPLETE" in result["reason_codes"]


def test_ten_complete_days_only_reach_operator_review() -> None:
    trade_dates = [
        "2026-06-29",
        "2026-06-30",
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-06",
        "2026-07-07",
        "2026-07-08",
        "2026-07-09",
        "2026-07-10",
    ]
    evidence = {
        component: {
            trade_date: {
                "passed": True,
                "run_id": f"{component}:{trade_date}",
            }
            for trade_date in trade_dates
        }
        for component in REQUIRED_COMPONENTS
    }

    result = evaluate_append_only_readiness(
        component_daily_evidence=evidence,
        configuration={"ready": True, "gates": {"observe_safe": True}},
        current_health={"ready": True},
    )

    assert result["status"] == "READY_FOR_OPERATOR_REVIEW"
    assert result["consecutive_qualified_trading_day_count"] == 10
    assert result["ready_for_operator_review"] is True
    assert result["official_krx_calendar_confirmation_required"] is True
    assert result["automatic_cutover_allowed"] is False
    assert result["flag_cleanup_allowed"] is False


def test_market_index_bootstrap_does_not_count_as_realtime_day() -> None:
    payload = {
        "status": "PASS",
        "checked_event_count": 2,
        "append_only_ready": 1,
        "no_trading_side_effects": 1,
        "realtime_source_count": 0,
        "tr_bootstrap_source_count": 2,
    }

    reasons = _projection_failure_reasons(
        "market_index",
        payload,
        run_date="2026-07-10",
        event_date="2026-07-10",
        zero_fields=("tr_bootstrap_source_count",),
        true_fields=(),
    )

    assert "TR_BOOTSTRAP_SOURCE_COUNT_NONZERO" in reasons
    assert "MARKET_INDEX_REALTIME_COVERAGE_INCOMPLETE" in reasons


def test_append_only_readiness_operator_and_dashboard_are_read_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "append-only-api.sqlite3"
    initialize_database(db_path).close()
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing-safe.env"))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "append-only-token")
    monkeypatch.setenv("TRADING_PROFILE", "OBSERVE")
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")

    with TestClient(app) as client:
        direct = client.get("/api/operator/append-only-readiness/status")
        dashboard = client.get(
            "/api/dashboard/snapshot",
            params={"fast": "true", "sections": "append_only_readiness"},
        )

    assert direct.status_code == 200
    assert direct.json()["status"] == "BLOCKED_CONFIG"
    assert direct.json()["read_only"] is True
    assert dashboard.status_code == 200
    assert dashboard.json()["append_only_readiness"]["status"] == "BLOCKED_CONFIG"
    assert dashboard.json()["append_only_readiness"]["flag_cleanup_allowed"] is False
