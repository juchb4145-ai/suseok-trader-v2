from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from gateway.event_factory import make_price_tick_event
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture


def test_strategy_api_evaluate_and_read_paths(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "strategy_api.sqlite3"))
    monkeypatch.setenv("STRATEGY_ENGINE_STALE_TICK_SEC", "999999999")
    connection = initialize_database(tmp_path / "strategy_api.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)
    connection.close()

    with TestClient(app) as client:
        status_before = client.get("/api/strategy/status")
        evaluated = client.post(
            f"/api/strategy/evaluate?candidate_instance_id={candidate_id}",
            headers={"X-Local-Token": "test-token"},
        )
        latest = client.get("/api/strategy/observations/latest")
        latest_for_candidate = client.get(f"/api/strategy/candidates/{candidate_id}")
        history = client.get(f"/api/strategy/candidates/{candidate_id}/history")
        observation_id = latest.json()["observations"][0]["strategy_observation_id"]
        setups = client.get(f"/api/strategy/observations/{observation_id}/setups")
        runs = client.get("/api/strategy/runs")
        errors = client.get("/api/strategy/errors")

    assert status_before.status_code == 200
    assert status_before.json()["observe_only"] is True
    assert evaluated.status_code == 200
    assert evaluated.json()["candidate_count"] == 1
    assert evaluated.json()["evaluated_count"] == 1
    assert evaluated.json()["matched_observation_count"] == 1
    assert evaluated.json()["observe_only"] is True
    assert latest.status_code == 200
    assert latest.json()["observations"][0]["overall_status"] == "MATCHED_OBSERVATION"
    assert latest_for_candidate.status_code == 200
    assert latest_for_candidate.json()["observation"]["setup_observations"]
    assert history.status_code == 200
    assert history.json()["observations"][0]["candidate_instance_id"] == candidate_id
    assert setups.status_code == 200
    assert len(setups.json()["setup_observations"]) == 4
    assert runs.status_code == 200
    assert runs.json()["runs"][0]["evaluated_count"] == 1
    assert errors.status_code == 200
    assert errors.json()["errors"] == []


def test_strategy_evaluate_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_CORE_TOKEN", "local-secret")
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "strategy_token.sqlite3"))

    with TestClient(app) as client:
        missing_token = client.post("/api/strategy/evaluate")
        with_token = client.post(
            "/api/strategy/evaluate",
            headers={"X-Core-Token": "local-secret"},
        )

    assert missing_token.status_code == 401
    assert with_token.status_code == 200


def test_strategy_integration_flow_stays_observe_only(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "strategy_flow.sqlite3"))
    monkeypatch.setenv("MARKET_DATA_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_SOURCE_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_EPISODE_TTL_SEC", "999999999")
    monkeypatch.setenv("STRATEGY_ENGINE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("THEME_MIN_ACTIVE_MEMBERS", "1")

    with TestClient(app) as client:
        headers = {"X-Local-Token": "test-token"}
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
            headers=headers,
        )
        theme_import = client.post("/api/themes/import", json=_theme_payload(), headers=headers)
        theme_rebuild = client.post(
            "/api/themes/snapshots/rebuild?theme_id=semiconductor",
            headers=headers,
        )
        first_rebuild = client.post("/api/candidates/rebuild", headers=headers)
        second_rebuild = client.post("/api/candidates/rebuild", headers=headers)
        candidates = client.get("/api/candidates")
        candidate = candidates.json()["candidates"][0]
        strategy = client.post("/api/strategy/evaluate", headers=headers)
        latest = client.get("/api/strategy/observations/latest")
        command_status = client.get("/api/gateway/commands/status")

    assert tick.json()["projection_status"] == "APPLIED"
    assert theme_import.status_code == 200
    assert theme_rebuild.status_code == 200
    assert first_rebuild.status_code == 200
    assert second_rebuild.status_code == 200
    assert candidate["state"] == "CONTEXT_READY"
    assert strategy.status_code == 200
    assert strategy.json()["evaluated_count"] == 1
    assert latest.json()["observations"][0]["overall_status"] == "MATCHED_OBSERVATION"
    assert command_status.json()["counts"]["QUEUED"] == 0


def _theme_payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "strategy_api_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [{"code": "005930", "name": "삼성전자"}],
            }
        ],
    }
