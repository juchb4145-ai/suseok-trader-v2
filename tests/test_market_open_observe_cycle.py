from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event, make_price_tick_event
from storage.sqlite import open_connection


def test_mock_events_project_and_observe_cycle_records_stage_updates(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    db_path = tmp_path / "market_open_observe_cycle.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("MARKET_DATA_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_SOURCE_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_EPISODE_TTL_SEC", "999999999")
    monkeypatch.setenv("STRATEGY_ENGINE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STRATEGY_STALE_SEC", "999999999")
    monkeypatch.setenv("ENTRY_TIMING_STALE_MAX_SECONDS", "999999999")

    with TestClient(app) as client:
        tick = client.post(
            "/api/gateway/events",
            json=make_price_tick_event(
                code="005930",
                name="삼성전자",
                price=97_000,
                change_rate=2.0,
                volume=10_000,
                trade_value=970_000_000,
                execution_strength=130.0,
                day_high=100_000,
                day_low=94_000,
            ).to_dict(),
        )
        condition = client.post(
            "/api/gateway/events",
            json=make_condition_event(code="005930", name="삼성전자", price=97_000).to_dict(),
        )
        latest_ticks = client.get("/api/market-data/ticks/latest")
        theme_import = client.post("/api/themes/import", json=_theme_payload())
        result = client.post("/api/operator/observe-cycle/run-once?trade_date=2026-06-29")
        latest_run = client.get("/api/operator/observe-cycle/runs/latest")
        commands = client.get("/api/gateway/commands/status")

    payload = result.json()
    stages = payload["stage_summary"]

    assert tick.json()["projection_status"] == "APPLIED"
    assert condition.json()["projection_status"] == "APPLIED"
    assert latest_ticks.json()["ticks"][0]["code"] == "005930"
    assert theme_import.status_code == 200
    assert result.status_code == 200
    assert stages["Theme"]["status"] in {"PASS", "WARN"}
    assert stages["Candidate"]["counts"]["active_candidate_count"] >= 1
    assert stages["Strategy"]["counts"]["evaluated_count"] >= 0
    assert stages["Risk"]["counts"]["evaluated_count"] >= 0
    assert stages["EntryTiming"]["counts"]["evaluated_count"] >= 1
    assert stages["CommandSafety"]["status"] == "PASS"
    assert payload["send_order_delta"] == 0
    assert payload["queue_commands"] is False
    assert latest_run.json()["run"]["run_id"] == payload["run_id"]
    assert commands.json()["counts"].get("QUEUED", 0) == 0

    connection = open_connection(db_path)
    try:
        send_order_count = connection.execute(
            "SELECT COUNT(*) AS count FROM gateway_commands WHERE command_type = 'send_order'"
        ).fetchone()["count"]
    finally:
        connection.close()
    assert send_order_count == 0


def test_observe_cycle_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "observe_cycle_token.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "local-secret")

    with TestClient(app) as client:
        missing = client.post("/api/operator/observe-cycle/run-once")
        wrong = client.post(
            "/api/operator/observe-cycle/run-once",
            headers={"X-Core-Token": "wrong"},
        )
        accepted = client.post(
            "/api/operator/observe-cycle/run-once",
            headers={"X-Core-Token": "local-secret"},
        )
        auth_probe_missing = client.get("/api/gateway/auth/probe")
        auth_probe_ok = client.get(
            "/api/gateway/auth/probe",
            headers={"X-Core-Token": "local-secret"},
        )

    assert missing.status_code == 401
    assert wrong.status_code == 403
    assert accepted.status_code == 200
    assert auth_probe_missing.status_code == 401
    assert auth_probe_ok.status_code == 200
    assert auth_probe_ok.json()["read_only"] is True


def _theme_payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "observe_cycle_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [
                    {"code": "005930", "name": "삼성전자"},
                    {"code": "000660", "name": "SK하이닉스"},
                ],
            }
        ],
    }
