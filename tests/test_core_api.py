from apps.core_api import app
from domain.ai_sidecar.policy import get_allowed_tasks, get_forbidden_actions
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


def test_ai_sidecar_status_returns_read_only_defaults(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("AI_SIDECAR_ENABLED", raising=False)
    monkeypatch.delenv("AI_SIDECAR_ALLOW_INTRADAY", raising=False)
    monkeypatch.delenv("AI_SIDECAR_ALLOW_ORDER_CONTEXT", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "core.sqlite3"))

    with TestClient(app) as client:
        response = client.get("/api/ai-sidecar/status")

    body = response.json()
    assert response.status_code == 200
    assert body["enabled"] is False
    assert body["allow_intraday"] is False
    assert body["allow_order_context"] is False
    assert body["openai_client_available"] is False
    assert body["execution_api_available"] is False
    assert body["allowed_tasks"] == get_allowed_tasks()
    assert body["forbidden_actions"] == get_forbidden_actions()


def test_ai_sidecar_tasks_returns_allowed_tasks(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "core.sqlite3"))

    with TestClient(app) as client:
        response = client.get("/api/ai-sidecar/tasks")

    assert response.status_code == 200
    assert response.json() == {"tasks": get_allowed_tasks()}


def test_ai_sidecar_insights_starts_empty_without_api_key(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "core.sqlite3"))

    with TestClient(app) as client:
        response = client.get("/api/ai-sidecar/insights")

    assert response.status_code == 200
    assert response.json() == {"insights": []}
