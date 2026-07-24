from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlencode

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent

from gateway.transport import (
    GatewayTransportError,
    JsonTransport,
    UrllibJsonTransport,
    make_token_headers,
)

_MIN_CORE_REQUEST_TIMEOUT_SEC = 6.0


class CoreClient:
    def __init__(
        self,
        *,
        core_url: str = "http://127.0.0.1:8000",
        token: str = "",
        timeout_sec: float = 6.0,
        transport: JsonTransport | None = None,
    ) -> None:
        self.core_url = core_url.rstrip("/")
        self.token = token
        self.timeout_sec = max(float(timeout_sec), _MIN_CORE_REQUEST_TIMEOUT_SEC)
        self._transport = transport or UrllibJsonTransport()

    def post_event(self, event: GatewayEvent) -> dict[str, Any]:
        return self._request_json(
            method="POST",
            path="/api/gateway/events",
            body=event.to_dict(),
        )

    def post_events(self, events: Sequence[GatewayEvent]) -> dict[str, Any]:
        payload = [event.to_dict() for event in events]
        if not payload:
            raise ValueError("events must not be empty")
        if len(payload) > 200:
            raise ValueError("events batch must contain at most 200 events")
        response = self._request_json(
            method="POST",
            path="/api/gateway/events/batch",
            body={"events": payload},
        )
        results = response.get("results")
        if not isinstance(results, list) or len(results) != len(payload):
            raise GatewayTransportError(
                "Core batch event response missing matching results list"
            )
        return response

    def poll_commands(self, *, limit: int = 20, wait_sec: float = 1.0) -> list[GatewayCommand]:
        query = urlencode({"limit": int(limit), "wait_sec": float(wait_sec)})
        response = self._request_json(
            method="GET",
            path=f"/api/gateway/commands?{query}",
            body=None,
        )
        commands = response.get("commands", [])
        if not isinstance(commands, list):
            raise GatewayTransportError("Core command poll response missing commands list")
        return [GatewayCommand.from_dict(command) for command in commands]

    def get_status(self) -> dict[str, Any]:
        return self._request_json(method="GET", path="/api/gateway/status", body=None)

    def close(self) -> None:
        return None

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **make_token_headers(self.token),
        }
        return self._transport.request_json(
            method=method,
            url=f"{self.core_url}{path}",
            body=body,
            headers=headers,
            timeout=self.timeout_sec,
        )
