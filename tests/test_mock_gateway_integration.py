from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from apps.core_api import app
from domain.broker.commands import GatewayCommand
from fastapi.testclient import TestClient
from gateway.core_client import CoreClient
from gateway.mock_runtime import MockGatewayRuntime
from gateway.settings import GatewaySettings
from gateway.transport import GatewayTransportError
from storage.gateway_command_store import GatewayCommandStatus, enqueue_command
from storage.sqlite import open_connection


class FastApiClientTransport:
    def __init__(self, client: TestClient) -> None:
        self.client = client

    def request_json(
        self,
        *,
        method: str,
        url: str,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> dict[str, Any]:
        parsed = urlsplit(url)
        path = parsed.path
        if parsed.query:
            path = f"{path}?{parsed.query}"
        response = self.client.request(
            method,
            path,
            json=body,
            headers=dict(headers),
        )
        if response.status_code >= 400:
            raise GatewayTransportError(f"Core HTTP {response.status_code}: {response.text}")
        return response.json()


def integration_settings() -> GatewaySettings:
    return GatewaySettings(
        core_url="http://testserver",
        poll_interval_sec=0.0,
        heartbeat_interval_sec=0.0,
        command_wait_sec=0.0,
        mock_price_tick_interval_sec=0.0,
    )


def test_mock_gateway_once_posts_events_to_core_api(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "integration.sqlite3"))

    with TestClient(app) as fastapi_client:
        core_client = CoreClient(
            core_url="http://testserver",
            transport=FastApiClientTransport(fastapi_client),
        )
        runtime = MockGatewayRuntime(settings=integration_settings(), client=core_client)

        runtime.start_once()

        recent = fastapi_client.get("/api/gateway/events/recent").json()["events"]
        status = fastapi_client.get("/api/gateway/status").json()
        latest_tick = fastapi_client.get("/api/market-data/ticks/005930").json()["tick"]
        bars = fastapi_client.get("/api/market-data/bars/005930?interval_sec=60").json()["bars"]
        conditions = fastapi_client.get("/api/market-data/conditions/recent").json()["signals"]

    event_types = {event["event_type"] for event in recent}
    assert {"heartbeat", "price_tick", "condition_event"}.issubset(event_types)
    assert status["last_heartbeat_at"] is not None
    assert status["recent_event_count"] == 3
    assert latest_tick["code"] == "005930"
    assert bars[0]["code"] == "005930"
    assert conditions[0]["code"] == "005930"


def test_mock_gateway_request_tr_command_updates_core_command_status(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    db_path = tmp_path / "integration.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))

    with TestClient(app) as fastapi_client:
        connection = open_connection(db_path)
        try:
            enqueue_command(
                connection,
                GatewayCommand(
                    command_id="cmd_integration_tr",
                    command_type="request_tr",
                    source="core",
                    payload={
                        "request_id": "tr_integration",
                        "tr_code": "OPT10001",
                        "request_name": "stock_basic",
                        "params": {"code": "005930"},
                    },
                ),
            )
        finally:
            connection.close()

        core_client = CoreClient(
            core_url="http://testserver",
            transport=FastApiClientTransport(fastapi_client),
        )
        runtime = MockGatewayRuntime(settings=integration_settings(), client=core_client)

        runtime.start_once()

        recent = fastapi_client.get("/api/gateway/events/recent").json()["events"]
        counts = fastapi_client.get("/api/gateway/commands/status").json()["counts"]

    event_types = [event["event_type"] for event in recent]
    assert "tr_response" in event_types
    assert "command_ack" in event_types
    assert counts[GatewayCommandStatus.ACKED.value] == 1
