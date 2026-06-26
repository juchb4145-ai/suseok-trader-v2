from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any, Protocol


class GatewayTransportError(RuntimeError):
    """Raised when the Gateway cannot exchange JSON with Core."""


class JsonTransport(Protocol):
    def request_json(
        self,
        *,
        method: str,
        url: str,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> dict[str, Any]:
        """Send an HTTP request and return a decoded JSON object."""


class UrllibJsonTransport:
    def request_json(
        self,
        *,
        method: str,
        url: str,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> dict[str, Any]:
        encoded_body = None
        if body is not None:
            encoded_body = json.dumps(body, ensure_ascii=False).encode("utf-8")

        request = urllib.request.Request(
            url=url,
            data=encoded_body,
            headers=dict(headers),
            method=method.upper(),
        )

        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = _read_error_body(exc)
            raise GatewayTransportError(
                f"Core HTTP {exc.code} for {method.upper()} {url}: {detail}"
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise GatewayTransportError(
                f"Core transport failed for {method.upper()} {url}: {exc}"
            ) from exc

        if not payload:
            return {}
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GatewayTransportError(
                f"Core returned invalid JSON for {method.upper()} {url}: {exc}"
            ) from exc
        if not isinstance(decoded, dict):
            raise GatewayTransportError(
                f"Core returned non-object JSON for {method.upper()} {url}"
            )
        return decoded


def make_token_headers(token: str) -> dict[str, str]:
    if not token:
        return {}
    return {"X-Core-Token": token}


def _read_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8")
    except Exception:
        return exc.reason or "HTTP error"
    return body or exc.reason or "HTTP error"
