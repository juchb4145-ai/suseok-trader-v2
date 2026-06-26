from __future__ import annotations

import io
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any

import pytest
from domain.broker.commands import GatewayCommand
from gateway.core_client import CoreClient
from gateway.event_factory import make_heartbeat_event
from gateway.transport import GatewayTransportError, UrllibJsonTransport


class RecordingTransport:
    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.response = response or {}
        self.calls: list[dict[str, Any]] = []

    def request_json(
        self,
        *,
        method: str,
        url: str,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "body": body,
                "headers": dict(headers),
                "timeout": timeout,
            }
        )
        return self.response


def test_core_client_post_event_sends_json_to_gateway_event_endpoint() -> None:
    transport = RecordingTransport({"accepted": True})
    client = CoreClient(
        core_url="http://core.local/",
        token="secret",
        timeout_sec=3.0,
        transport=transport,
    )

    result = client.post_event(make_heartbeat_event(source="test-gateway"))

    assert result == {"accepted": True}
    call = transport.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "http://core.local/api/gateway/events"
    assert call["body"]["event_type"] == "heartbeat"
    assert call["headers"]["X-Core-Token"] == "secret"
    assert call["timeout"] == 3.0


def test_core_client_poll_commands_converts_gateway_command_list() -> None:
    command = GatewayCommand(
        command_id="cmd_client_poll",
        command_type="request_tr",
        source="core",
        payload={"tr_code": "OPT10001", "params": {"code": "005930"}},
    )
    transport = RecordingTransport({"commands": [command.to_dict()]})
    client = CoreClient(core_url="http://core.local", transport=transport)

    commands = client.poll_commands(limit=5, wait_sec=0.5)

    assert commands == [command]
    assert transport.calls[0]["method"] == "GET"
    assert transport.calls[0]["url"].endswith("/api/gateway/commands?limit=5&wait_sec=0.5")


def test_core_client_rejects_malformed_command_response() -> None:
    client = CoreClient(
        core_url="http://core.local",
        transport=RecordingTransport({"commands": {}}),
    )

    with pytest.raises(GatewayTransportError, match="commands list"):
        client.poll_commands()


def test_urllib_transport_wraps_http_errors(monkeypatch) -> None:
    def raise_http_error(*_: object, **__: object) -> object:
        raise urllib.error.HTTPError(
            url="http://core.local/api/gateway/events",
            code=500,
            msg="boom",
            hdrs={},
            fp=io.BytesIO(b'{"detail":"bad"}'),
        )

    monkeypatch.setattr(urllib.request, "urlopen", raise_http_error)

    with pytest.raises(GatewayTransportError, match="Core HTTP 500"):
        UrllibJsonTransport().request_json(
            method="POST",
            url="http://core.local/api/gateway/events",
            body={"event_type": "heartbeat"},
            headers={},
            timeout=1.0,
        )
