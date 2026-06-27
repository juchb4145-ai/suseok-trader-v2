from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from domain.market.bars import normalize_interval_list

DEFAULT_DB_PATH = "storage/suseok-trader-v2.sqlite3"


class TradingMode(StrEnum):
    OBSERVE = "OBSERVE"
    LIVE_SIM = "LIVE_SIM"
    LIVE_REAL = "LIVE_REAL"


@dataclass(frozen=True)
class Settings:
    trading_mode: TradingMode = TradingMode.OBSERVE
    trading_core_token: str = ""
    trading_db_path: Path = Path("storage/suseok-trader-v2.sqlite3")
    trading_allow_live_sim: bool = False
    trading_allow_live_real: bool = False
    ai_sidecar_enabled_value: bool = False
    ai_sidecar_allow_intraday: bool = False
    ai_sidecar_allow_order_context: bool = False
    ai_sidecar_model: str = ""
    ai_sidecar_max_context_chars: int = 12000
    ai_sidecar_request_timeout_sec: int = 30
    market_data_enabled: bool = True
    market_data_tick_stale_sec: int = 10
    market_data_degraded_tick_stale_sec: int = 30
    market_data_bar_intervals_sec: tuple[int, ...] = (60, 180, 300)
    market_data_rebuild_batch_size: int = 500
    market_data_max_recent_ticks: int = 1000

    def __post_init__(self) -> None:
        if self.market_data_degraded_tick_stale_sec < self.market_data_tick_stale_sec:
            raise ValueError(
                "MARKET_DATA_DEGRADED_TICK_STALE_SEC must be >= MARKET_DATA_TICK_STALE_SEC"
            )

    @property
    def live_sim_allowed(self) -> bool:
        return self.trading_allow_live_sim

    @property
    def live_real_allowed(self) -> bool:
        return self.trading_allow_live_real

    @property
    def ai_sidecar_enabled(self) -> bool:
        return self.ai_sidecar_enabled_value

    @property
    def ai_sidecar_intraday_allowed(self) -> bool:
        return self.ai_sidecar_allow_intraday

    @property
    def ai_sidecar_order_context_allowed(self) -> bool:
        return self.ai_sidecar_allow_order_context


_TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "f", "no", "n", "off", ""}


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if environ is None else environ

    return Settings(
        trading_mode=_parse_trading_mode(env.get("TRADING_MODE", TradingMode.OBSERVE.value)),
        trading_core_token=env.get("TRADING_CORE_TOKEN", ""),
        trading_db_path=Path(env.get("TRADING_DB_PATH", DEFAULT_DB_PATH)).expanduser(),
        trading_allow_live_sim=_parse_bool(env.get("TRADING_ALLOW_LIVE_SIM", "false")),
        trading_allow_live_real=_parse_bool(env.get("TRADING_ALLOW_LIVE_REAL", "false")),
        ai_sidecar_enabled_value=_parse_bool(env.get("AI_SIDECAR_ENABLED", "false")),
        ai_sidecar_allow_intraday=_parse_bool(env.get("AI_SIDECAR_ALLOW_INTRADAY", "false")),
        ai_sidecar_allow_order_context=_parse_bool(
            env.get("AI_SIDECAR_ALLOW_ORDER_CONTEXT", "false")
        ),
        ai_sidecar_model=env.get("AI_SIDECAR_MODEL", ""),
        ai_sidecar_max_context_chars=_parse_int(
            env.get("AI_SIDECAR_MAX_CONTEXT_CHARS", "12000"),
            "AI_SIDECAR_MAX_CONTEXT_CHARS",
            min_value=1,
        ),
        ai_sidecar_request_timeout_sec=_parse_int(
            env.get("AI_SIDECAR_REQUEST_TIMEOUT_SEC", "30"),
            "AI_SIDECAR_REQUEST_TIMEOUT_SEC",
            min_value=1,
        ),
        market_data_enabled=_parse_bool(env.get("MARKET_DATA_ENABLED", "true")),
        market_data_tick_stale_sec=_parse_int(
            env.get("MARKET_DATA_TICK_STALE_SEC", "10"),
            "MARKET_DATA_TICK_STALE_SEC",
            min_value=1,
        ),
        market_data_degraded_tick_stale_sec=_parse_int(
            env.get("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "30"),
            "MARKET_DATA_DEGRADED_TICK_STALE_SEC",
            min_value=1,
        ),
        market_data_bar_intervals_sec=_parse_intervals(
            env.get("MARKET_DATA_BAR_INTERVALS_SEC", "60,180,300")
        ),
        market_data_rebuild_batch_size=_parse_int(
            env.get("MARKET_DATA_REBUILD_BATCH_SIZE", "500"),
            "MARKET_DATA_REBUILD_BATCH_SIZE",
            min_value=1,
        ),
        market_data_max_recent_ticks=_parse_int(
            env.get("MARKET_DATA_MAX_RECENT_TICKS", "1000"),
            "MARKET_DATA_MAX_RECENT_TICKS",
            min_value=1,
        ),
    )


def _parse_trading_mode(value: str) -> TradingMode:
    normalized = value.strip().upper()
    try:
        return TradingMode(normalized)
    except ValueError as exc:
        allowed = ", ".join(mode.value for mode in TradingMode)
        raise ValueError(f"Unsupported TRADING_MODE={value!r}; expected one of: {allowed}") from exc


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def _parse_int(value: str, field_name: str, *, min_value: int | None = None) -> int:
    normalized = value.strip()
    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise ValueError(f"Unsupported integer value for {field_name}: {value!r}") from exc

    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field_name} must be >= {min_value}")
    return parsed


def _parse_intervals(value: str) -> tuple[int, ...]:
    parts = [part.strip() for part in value.split(",")]
    if any(part == "" for part in parts):
        raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC must be a comma-separated integer list")
    try:
        intervals = [int(part) for part in parts]
    except ValueError as exc:
        raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC must contain integers") from exc
    return normalize_interval_list(intervals)
