from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event, make_price_tick_event


def test_candidate_api_rebuild_and_read_paths(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "candidate_api.sqlite3"))
    monkeypatch.setenv("MARKET_DATA_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_SOURCE_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_EPISODE_TTL_SEC", "999999999")

    with TestClient(app) as client:
        headers = {"X-Local-Token": "test-token"}
        tick = client.post(
            "/api/gateway/events",
            json=make_price_tick_event(
                code="005930",
                name="삼성전자",
                change_rate=1.0,
                trade_value=100_000_000,
            ).to_dict(),
            headers=headers,
        )
        condition = client.post(
            "/api/gateway/events",
            json=make_condition_event(code="005930", name="삼성전자").to_dict(),
            headers=headers,
        )
        theme_import = client.post("/api/themes/import", json=_theme_payload(), headers=headers)
        theme_rebuild = client.post(
            "/api/themes/snapshots/rebuild?theme_id=semiconductor",
            headers=headers,
        )
        candidate_rebuild = client.post("/api/candidates/rebuild", headers=headers)

        status = client.get("/api/candidates/status")
        candidates = client.get("/api/candidates")
        candidate_id = candidates.json()["candidates"][0]["candidate_instance_id"]
        detail = client.get(
            f"/api/candidates/{candidate_id}"
            "?include_context=true&include_sources=true&include_transitions=true"
        )
        sources = client.get(f"/api/candidates/{candidate_id}/sources")
        transitions = client.get(f"/api/candidates/{candidate_id}/transitions")
        by_code = client.get("/api/candidates/by-code/A005930")
        errors = client.get("/api/candidates/projection-errors")

    assert tick.json()["projection_status"] == "APPLIED"
    assert condition.json()["projection_status"] == "APPLIED"
    assert theme_import.status_code == 200
    assert theme_rebuild.status_code == 200
    assert candidate_rebuild.status_code == 200
    assert candidate_rebuild.json()["candidate_created_count"] == 1
    assert candidate_rebuild.json()["source_event_count"] == 2

    assert status.status_code == 200
    assert status.json()["enabled"] is True
    assert status.json()["candidate_count"] == 1
    assert status.json()["projection_error_count"] == 0
    assert candidates.status_code == 200
    assert candidates.json()["candidates"][0]["code"] == "005930"
    assert detail.status_code == 200
    assert detail.json()["candidate"]["context"]["readiness"]["has_1m_bar"] is True
    assert len(detail.json()["candidate"]["sources"]) == 2
    assert detail.json()["candidate"]["transitions"][0]["to_state"] == "DETECTED"
    assert sources.status_code == 200
    assert len(sources.json()["sources"]) == 2
    assert transitions.status_code == 200
    assert transitions.json()["transitions"][-1]["to_state"] == "WATCHING"
    assert by_code.status_code == 200
    assert by_code.json()["candidates"][0]["candidate_instance_id"] == candidate_id
    assert errors.status_code == 200
    assert errors.json()["errors"] == []


def test_candidate_rebuild_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_CORE_TOKEN", "local-secret")
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "candidate_token.sqlite3"))

    with TestClient(app) as client:
        missing_token = client.post("/api/candidates/rebuild")
        with_token = client.post(
            "/api/candidates/rebuild",
            headers={"X-Core-Token": "local-secret"},
        )

    assert missing_token.status_code == 401
    assert with_token.status_code == 200


def _theme_payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "candidate_api_fixture",
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
