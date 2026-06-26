from apps.core_api import app
from fastapi.testclient import TestClient


def test_health_returns_ok(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "core.sqlite3"))

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_status_returns_observe_and_default_live_flags(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.delenv("TRADING_ALLOW_LIVE_SIM", raising=False)
    monkeypatch.delenv("TRADING_ALLOW_LIVE_REAL", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "core.sqlite3"))

    with TestClient(app) as client:
        response = client.get("/api/status")

    assert response.status_code == 200
    assert response.json()["mode"] == "OBSERVE"
    assert response.json()["live_sim_allowed"] is False
    assert response.json()["live_real_allowed"] is False
