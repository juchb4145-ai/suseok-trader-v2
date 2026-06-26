from __future__ import annotations

from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.commands import GatewayCommand
from fastapi.testclient import TestClient
from storage.gateway_command_store import GatewayCommandStatus, enqueue_command
from storage.sqlite import open_connection

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC).isoformat()


def heartbeat_event(event_id: str = "evt_api_heartbeat") -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "heartbeat",
        "source": "test-gateway",
        "ts": TS,
        "payload": {"status": "ok"},
    }


def test_gateway_event_api_accepts_duplicates_and_lists_recent(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        first = client.post("/api/gateway/events", json=heartbeat_event())
        duplicate = client.post("/api/gateway/events", json=heartbeat_event())
        recent = client.get("/api/gateway/events/recent")
        gateway_status = client.get("/api/gateway/status")

    assert first.status_code == 200
    assert first.json()["accepted"] is True
    assert first.json()["duplicate"] is False
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert recent.status_code == 200
    assert recent.json()["events"][0]["event_id"] == "evt_api_heartbeat"
    assert gateway_status.status_code == 200
    assert gateway_status.json()["last_heartbeat_at"] is not None
    assert gateway_status.json()["recent_event_count"] == 1
    assert gateway_status.json()["order_commands_allowed"] is False


def test_gateway_commands_api_dispatches_queued_commands(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    db_path = tmp_path / "api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))

    with TestClient(app) as client:
        connection = open_connection(db_path)
        try:
            enqueue_command(
                connection,
                GatewayCommand(
                    command_id="cmd_api_poll",
                    command_type="request_tr",
                    source="core",
                    payload={"tr_code": "OPT10001", "params": {"code": "005930"}},
                ),
            )
        finally:
            connection.close()

        response = client.get("/api/gateway/commands")
        status_response = client.get("/api/gateway/commands/status")

    assert response.status_code == 200
    assert response.json()["commands"][0]["command_id"] == "cmd_api_poll"
    assert response.json()["commands"][0]["command_type"] == "request_tr"
    assert status_response.status_code == 200
    assert status_response.json()["counts"][GatewayCommandStatus.DISPATCHED.value] == 1


def test_gateway_event_api_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")

    with TestClient(app) as client:
        missing = client.post("/api/gateway/events", json=heartbeat_event("evt_missing_token"))
        wrong = client.post(
            "/api/gateway/events",
            json=heartbeat_event("evt_wrong_token"),
            headers={"X-Local-Token": "wrong"},
        )
        accepted = client.post(
            "/api/gateway/events",
            json=heartbeat_event("evt_with_token"),
            headers={"X-Core-Token": "secret-token"},
        )
        read_only_status = client.get("/api/gateway/status")

    assert missing.status_code == 401
    assert wrong.status_code == 403
    assert accepted.status_code == 200
    assert read_only_status.status_code == 200
    assert read_only_status.json()["token_required"] is True
