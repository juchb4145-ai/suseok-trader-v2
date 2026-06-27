from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from gateway.event_factory import make_price_tick_event
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture


def test_risk_api_evaluate_and_read_paths(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "risk_api.sqlite3"))
    monkeypatch.setenv("STRATEGY_ENGINE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STRATEGY_STALE_SEC", "999999999")
    connection = initialize_database(tmp_path / "risk_api.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)
    connection.close()

    with TestClient(app) as client:
        status_before = client.get("/api/risk/status")
        strategy = client.post(f"/api/strategy/evaluate?candidate_instance_id={candidate_id}")
        evaluated = client.post(f"/api/risk/evaluate?candidate_instance_id={candidate_id}")
        latest = client.get("/api/risk/observations/latest")
        latest_for_candidate = client.get(f"/api/risk/candidates/{candidate_id}")
        history = client.get(f"/api/risk/candidates/{candidate_id}/history")
        observation_id = latest.json()["observations"][0]["risk_observation_id"]
        observation = client.get(f"/api/risk/observations/{observation_id}")
        checks = client.get(f"/api/risk/observations/{observation_id}/checks")
        runs = client.get("/api/risk/runs")
        errors = client.get("/api/risk/errors")

    assert status_before.status_code == 200
    assert status_before.json()["observe_only"] is True
    assert status_before.json()["order_routing_enabled"] is False
    assert strategy.status_code == 200
    assert evaluated.status_code == 200
    assert evaluated.json()["evaluated_count"] == 1
    assert evaluated.json()["observe_pass_count"] == 1
    assert evaluated.json()["observe_only"] is True
    assert evaluated.json()["order_routing_enabled"] is False
    assert latest.status_code == 200
    assert latest.json()["observations"][0]["overall_status"] == "OBSERVE_PASS"
    assert latest_for_candidate.status_code == 200
    assert latest_for_candidate.json()["observation"]["check_observations"]
    assert history.status_code == 200
    assert history.json()["observations"][0]["candidate_instance_id"] == candidate_id
    assert observation.status_code == 200
    assert observation.json()["observation"]["observe_only"] is True
    assert checks.status_code == 200
    assert len(checks.json()["check_observations"]) >= 5
    assert runs.status_code == 200
    assert runs.json()["runs"][0]["evaluated_count"] == 1
    assert errors.status_code == 200
    assert errors.json()["errors"] == []


def test_risk_evaluate_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_CORE_TOKEN", "local-secret")
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "risk_token.sqlite3"))

    with TestClient(app) as client:
        missing_token = client.post("/api/risk/evaluate")
        with_token = client.post(
            "/api/risk/evaluate",
            headers={"X-Core-Token": "local-secret"},
        )

    assert missing_token.status_code == 401
    assert with_token.status_code == 200


def test_risk_integration_flow_stays_observe_only(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "risk_flow.sqlite3"))
    monkeypatch.setenv("MARKET_DATA_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_SOURCE_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_EPISODE_TTL_SEC", "999999999")
    monkeypatch.setenv("STRATEGY_ENGINE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STRATEGY_STALE_SEC", "999999999")
    monkeypatch.setenv("THEME_MIN_ACTIVE_MEMBERS", "1")

    with TestClient(app) as client:
        tick = client.post(
            "/api/gateway/events",
            json=make_price_tick_event(
                code="005930",
                name="삼성전자",
                price=97_000,
                day_high=100_000,
                day_low=94_000,
                change_rate=2.0,
                trade_value=97_000_000,
            ).to_dict(),
        )
        theme_import = client.post("/api/themes/import", json=_theme_payload())
        theme_rebuild = client.post("/api/themes/snapshots/rebuild?theme_id=semiconductor")
        first_rebuild = client.post("/api/candidates/rebuild")
        second_rebuild = client.post("/api/candidates/rebuild")
        candidates = client.get("/api/candidates")
        candidate = candidates.json()["candidates"][0]
        strategy = client.post("/api/strategy/evaluate")
        risk = client.post("/api/risk/evaluate")
        latest = client.get("/api/risk/observations/latest")
        command_status = client.get("/api/gateway/commands/status")

    assert tick.json()["projection_status"] == "APPLIED"
    assert theme_import.status_code == 200
    assert theme_rebuild.status_code == 200
    assert first_rebuild.status_code == 200
    assert second_rebuild.status_code == 200
    assert candidate["state"] == "CONTEXT_READY"
    assert strategy.status_code == 200
    assert strategy.json()["evaluated_count"] == 1
    assert risk.status_code == 200
    assert risk.json()["evaluated_count"] == 1
    assert latest.json()["observations"][0]["observe_only"] is True
    assert latest.json()["observations"][0]["overall_status"] in {
        "OBSERVE_PASS",
        "OBSERVE_CAUTION",
        "DATA_WAIT",
    }
    assert command_status.json()["counts"]["QUEUED"] == 0


def _theme_payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "risk_api_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [{"code": "005930", "name": "삼성전자"}],
            }
        ],
    }
