from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class GatewaySettings:
    core_url: str = "http://127.0.0.1:8000"
    core_token: str = ""
    source: str = "mock_gateway"
    poll_interval_sec: float = 1.0
    heartbeat_interval_sec: float = 2.0
    event_timeout_sec: float = 5.0
    command_wait_sec: float = 1.0
    command_limit: int = 20
    mock_once: bool = False
    mock_price_tick_interval_sec: float = 2.0


_TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "f", "no", "n", "off", ""}


def load_gateway_settings(environ: Mapping[str, str] | None = None) -> GatewaySettings:
    env = os.environ if environ is None else environ
    return GatewaySettings(
        core_url=_strip_non_empty(
            env.get("GATEWAY_CORE_URL", "http://127.0.0.1:8000"),
            "GATEWAY_CORE_URL",
        ),
        core_token=env.get("GATEWAY_CORE_TOKEN", ""),
        source=_strip_non_empty(env.get("GATEWAY_SOURCE", "mock_gateway"), "GATEWAY_SOURCE"),
        poll_interval_sec=_parse_float(
            env.get("GATEWAY_POLL_INTERVAL_SEC", "1.0"),
            "GATEWAY_POLL_INTERVAL_SEC",
            min_value=0.0,
        ),
        heartbeat_interval_sec=_parse_float(
            env.get("GATEWAY_HEARTBEAT_INTERVAL_SEC", "2.0"),
            "GATEWAY_HEARTBEAT_INTERVAL_SEC",
            min_value=0.0,
        ),
        event_timeout_sec=_parse_float(
            env.get("GATEWAY_EVENT_TIMEOUT_SEC", "5.0"),
            "GATEWAY_EVENT_TIMEOUT_SEC",
            min_value=0.1,
        ),
        command_wait_sec=_parse_float(
            env.get("GATEWAY_COMMAND_WAIT_SEC", "1.0"),
            "GATEWAY_COMMAND_WAIT_SEC",
            min_value=0.0,
        ),
        command_limit=_parse_int(
            env.get("GATEWAY_COMMAND_LIMIT", "20"),
            "GATEWAY_COMMAND_LIMIT",
            min_value=1,
        ),
        mock_once=_parse_bool(env.get("GATEWAY_MOCK_ONCE", "false")),
        mock_price_tick_interval_sec=_parse_float(
            env.get("GATEWAY_MOCK_PRICE_TICK_INTERVAL_SEC", "2.0"),
            "GATEWAY_MOCK_PRICE_TICK_INTERVAL_SEC",
            min_value=0.0,
        ),
    )


def _strip_non_empty(value: str, field_name: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} must not be empty")
    return stripped


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def _parse_float(value: str, field_name: str, *, min_value: float | None = None) -> float:
    try:
        parsed = float(value.strip())
    except ValueError as exc:
        raise ValueError(f"Unsupported float value for {field_name}: {value!r}") from exc
    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field_name} must be >= {min_value}")
    return parsed


def _parse_int(value: str, field_name: str, *, min_value: int | None = None) -> int:
    try:
        parsed = int(value.strip())
    except ValueError as exc:
        raise ValueError(f"Unsupported integer value for {field_name}: {value!r}") from exc
    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field_name} must be >= {min_value}")
    return parsed

