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
    event_timeout_sec: float = 6.0
    command_wait_sec: float = 0.0
    command_limit: int = 20
    mock_once: bool = False
    mock_price_tick_interval_sec: float = 2.0
    kiwoom_market_index_enabled: bool = False
    kiwoom_market_index_realtime_enabled: bool = False
    kiwoom_market_index_tr_bootstrap_enabled: bool = False
    kiwoom_market_index_codes: tuple[str, ...] = ("KOSPI", "KOSDAQ")
    kiwoom_market_index_screen_no: str = "5700"
    kiwoom_market_index_poll_sec: float = 60.0


_TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "f", "no", "n", "off", ""}
_MIN_EVENT_TIMEOUT_SEC = 6.0


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
        event_timeout_sec=max(
            _parse_float(
                env.get("GATEWAY_EVENT_TIMEOUT_SEC", "6.0"),
                "GATEWAY_EVENT_TIMEOUT_SEC",
                min_value=0.1,
            ),
            _MIN_EVENT_TIMEOUT_SEC,
        ),
        command_wait_sec=_parse_float(
            env.get("GATEWAY_COMMAND_WAIT_SEC", "0.0"),
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
        kiwoom_market_index_enabled=_parse_bool(
            env.get("KIWOOM_MARKET_INDEX_ENABLED", "false")
        ),
        kiwoom_market_index_realtime_enabled=_parse_bool(
            env.get("KIWOOM_MARKET_INDEX_REALTIME_ENABLED", "false")
        ),
        kiwoom_market_index_tr_bootstrap_enabled=_parse_bool(
            env.get("KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED", "false")
        ),
        kiwoom_market_index_codes=_parse_csv_list(
            env.get("KIWOOM_MARKET_INDEX_CODES", "KOSPI,KOSDAQ"),
            "KIWOOM_MARKET_INDEX_CODES",
        ),
        kiwoom_market_index_screen_no=_strip_non_empty(
            env.get("KIWOOM_MARKET_INDEX_SCREEN_NO", "5700"),
            "KIWOOM_MARKET_INDEX_SCREEN_NO",
        ),
        kiwoom_market_index_poll_sec=_parse_float(
            env.get("KIWOOM_MARKET_INDEX_POLL_SEC", "60.0"),
            "KIWOOM_MARKET_INDEX_POLL_SEC",
            min_value=1.0,
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


def _parse_csv_list(value: str, field_name: str) -> tuple[str, ...]:
    parts = tuple(part.strip().upper() for part in str(value or "").split(","))
    if any(part == "" for part in parts):
        raise ValueError(f"{field_name} must be a comma-separated non-empty list")
    if len(set(parts)) != len(parts):
        raise ValueError(f"{field_name} must not contain duplicates")
    return parts
