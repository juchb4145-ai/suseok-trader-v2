from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta, timezone, tzinfo
from enum import StrEnum
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    theme_service_enabled: bool = True
    theme_min_active_members: int = 2
    theme_min_fresh_coverage_ratio: float = 0.3
    theme_leading_rising_ratio: float = 0.5
    theme_spreading_rising_ratio: float = 0.35
    theme_min_total_trade_value: float = 0.0
    theme_leader_min_change_rate: float = 0.0
    theme_leader_min_trade_value_delta_1m: float = 0.0
    theme_co_leader_score_ratio: float = 0.8
    theme_snapshot_max_members: int = 200
    theme_import_allow_replace: bool = False
    candidate_fsm_enabled: bool = True
    candidate_trade_date_timezone: str = "Asia/Seoul"
    candidate_source_stale_sec: int = 300
    candidate_tick_stale_sec: int = 30
    candidate_episode_ttl_sec: int = 1800
    candidate_context_require_1m_bar: bool = True
    candidate_context_require_vwap: bool = False
    candidate_max_active_per_code: int = 1
    candidate_theme_source_states: tuple[str, ...] = ("LEADING", "SPREADING")
    candidate_theme_member_roles: tuple[str, ...] = (
        "LEADER_CANDIDATE",
        "CO_LEADER_CANDIDATE",
        "FOLLOWER_CANDIDATE",
    )
    candidate_condition_action_enter: str = "ENTER"
    candidate_condition_action_exit: str = "EXIT"
    strategy_engine_enabled: bool = True
    strategy_engine_observe_only: bool = True
    strategy_engine_max_candidates: int = 500
    strategy_engine_require_context_ready: bool = True
    strategy_engine_allowed_candidate_states: tuple[str, ...] = ("CONTEXT_READY", "WATCHING")
    strategy_engine_stale_tick_sec: int = 30
    strategy_engine_allowed_theme_states: tuple[str, ...] = ("LEADING", "SPREADING")
    strategy_engine_allowed_theme_roles: tuple[str, ...] = (
        "LEADER_CANDIDATE",
        "CO_LEADER_CANDIDATE",
        "FOLLOWER_CANDIDATE",
    )
    strategy_engine_require_1m_bar: bool = True
    strategy_engine_require_vwap: bool = False
    strategy_pullback_min_pct: float = 0.3
    strategy_pullback_max_pct: float = 5.0
    strategy_vwap_reclaim_tolerance_pct: float = 1.0
    strategy_min_trade_value_delta_1m: float = 0.0
    strategy_min_trade_value_delta_3m: float = 0.0
    strategy_breakout_retest_near_high_pct: float = 2.0
    strategy_follower_expansion_min_theme_rising_ratio: float = 0.35
    strategy_config_version: str = "observe_v1"

    def __post_init__(self) -> None:
        if self.market_data_degraded_tick_stale_sec < self.market_data_tick_stale_sec:
            raise ValueError(
                "MARKET_DATA_DEGRADED_TICK_STALE_SEC must be >= MARKET_DATA_TICK_STALE_SEC"
            )
        for field_name in (
            "theme_min_fresh_coverage_ratio",
            "theme_leading_rising_ratio",
            "theme_spreading_rising_ratio",
            "theme_co_leader_score_ratio",
        ):
            _validate_ratio(getattr(self, field_name), field_name.upper())
        if self.theme_min_active_members < 1:
            raise ValueError("THEME_MIN_ACTIVE_MEMBERS must be >= 1")
        if self.theme_snapshot_max_members < 1:
            raise ValueError("THEME_SNAPSHOT_MAX_MEMBERS must be >= 1")
        if self.theme_min_total_trade_value < 0:
            raise ValueError("THEME_MIN_TOTAL_TRADE_VALUE must be >= 0")
        if self.theme_leader_min_trade_value_delta_1m < 0:
            raise ValueError("THEME_LEADER_MIN_TRADE_VALUE_DELTA_1M must be >= 0")
        _validate_timezone(self.candidate_trade_date_timezone)
        for field_name in (
            "candidate_source_stale_sec",
            "candidate_tick_stale_sec",
            "candidate_episode_ttl_sec",
            "candidate_max_active_per_code",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if not self.candidate_theme_source_states:
            raise ValueError("CANDIDATE_THEME_SOURCE_STATES must not be empty")
        if not self.candidate_theme_member_roles:
            raise ValueError("CANDIDATE_THEME_MEMBER_ROLES must not be empty")
        object.__setattr__(
            self,
            "candidate_theme_source_states",
            _normalize_list_values(self.candidate_theme_source_states),
        )
        object.__setattr__(
            self,
            "candidate_theme_member_roles",
            _normalize_list_values(self.candidate_theme_member_roles),
        )
        object.__setattr__(
            self,
            "candidate_condition_action_enter",
            _normalize_non_empty(self.candidate_condition_action_enter),
        )
        object.__setattr__(
            self,
            "candidate_condition_action_exit",
            _normalize_non_empty(self.candidate_condition_action_exit),
        )
        for field_name in ("strategy_engine_max_candidates", "strategy_engine_stale_tick_sec"):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        for field_name in (
            "strategy_pullback_min_pct",
            "strategy_pullback_max_pct",
            "strategy_vwap_reclaim_tolerance_pct",
            "strategy_min_trade_value_delta_1m",
            "strategy_min_trade_value_delta_3m",
            "strategy_breakout_retest_near_high_pct",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        if self.strategy_pullback_max_pct < self.strategy_pullback_min_pct:
            raise ValueError("STRATEGY_PULLBACK_MAX_PCT must be >= STRATEGY_PULLBACK_MIN_PCT")
        _validate_ratio(
            self.strategy_follower_expansion_min_theme_rising_ratio,
            "STRATEGY_FOLLOWER_EXPANSION_MIN_THEME_RISING_RATIO",
        )
        if not self.strategy_engine_allowed_candidate_states:
            raise ValueError("STRATEGY_ENGINE_ALLOWED_CANDIDATE_STATES must not be empty")
        if not self.strategy_engine_allowed_theme_states:
            raise ValueError("STRATEGY_ENGINE_ALLOWED_THEME_STATES must not be empty")
        if not self.strategy_engine_allowed_theme_roles:
            raise ValueError("STRATEGY_ENGINE_ALLOWED_THEME_ROLES must not be empty")
        object.__setattr__(
            self,
            "strategy_engine_allowed_candidate_states",
            _normalize_list_values(self.strategy_engine_allowed_candidate_states),
        )
        object.__setattr__(
            self,
            "strategy_engine_allowed_theme_states",
            _normalize_list_values(self.strategy_engine_allowed_theme_states),
        )
        object.__setattr__(
            self,
            "strategy_engine_allowed_theme_roles",
            _normalize_list_values(self.strategy_engine_allowed_theme_roles),
        )
        object.__setattr__(
            self,
            "strategy_config_version",
            _require_non_empty_config(self.strategy_config_version),
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
        theme_service_enabled=_parse_bool(env.get("THEME_SERVICE_ENABLED", "true")),
        theme_min_active_members=_parse_int(
            env.get("THEME_MIN_ACTIVE_MEMBERS", "2"),
            "THEME_MIN_ACTIVE_MEMBERS",
            min_value=1,
        ),
        theme_min_fresh_coverage_ratio=_parse_float(
            env.get("THEME_MIN_FRESH_COVERAGE_RATIO", "0.3"),
            "THEME_MIN_FRESH_COVERAGE_RATIO",
        ),
        theme_leading_rising_ratio=_parse_float(
            env.get("THEME_LEADING_RISING_RATIO", "0.5"),
            "THEME_LEADING_RISING_RATIO",
        ),
        theme_spreading_rising_ratio=_parse_float(
            env.get("THEME_SPREADING_RISING_RATIO", "0.35"),
            "THEME_SPREADING_RISING_RATIO",
        ),
        theme_min_total_trade_value=_parse_float(
            env.get("THEME_MIN_TOTAL_TRADE_VALUE", "0"),
            "THEME_MIN_TOTAL_TRADE_VALUE",
            min_value=0.0,
        ),
        theme_leader_min_change_rate=_parse_float(
            env.get("THEME_LEADER_MIN_CHANGE_RATE", "0.0"),
            "THEME_LEADER_MIN_CHANGE_RATE",
        ),
        theme_leader_min_trade_value_delta_1m=_parse_float(
            env.get("THEME_LEADER_MIN_TRADE_VALUE_DELTA_1M", "0"),
            "THEME_LEADER_MIN_TRADE_VALUE_DELTA_1M",
            min_value=0.0,
        ),
        theme_co_leader_score_ratio=_parse_float(
            env.get("THEME_CO_LEADER_SCORE_RATIO", "0.8"),
            "THEME_CO_LEADER_SCORE_RATIO",
        ),
        theme_snapshot_max_members=_parse_int(
            env.get("THEME_SNAPSHOT_MAX_MEMBERS", "200"),
            "THEME_SNAPSHOT_MAX_MEMBERS",
            min_value=1,
        ),
        theme_import_allow_replace=_parse_bool(env.get("THEME_IMPORT_ALLOW_REPLACE", "false")),
        candidate_fsm_enabled=_parse_bool(env.get("CANDIDATE_FSM_ENABLED", "true")),
        candidate_trade_date_timezone=env.get("CANDIDATE_TRADE_DATE_TIMEZONE", "Asia/Seoul"),
        candidate_source_stale_sec=_parse_int(
            env.get("CANDIDATE_SOURCE_STALE_SEC", "300"),
            "CANDIDATE_SOURCE_STALE_SEC",
            min_value=1,
        ),
        candidate_tick_stale_sec=_parse_int(
            env.get("CANDIDATE_TICK_STALE_SEC", "30"),
            "CANDIDATE_TICK_STALE_SEC",
            min_value=1,
        ),
        candidate_episode_ttl_sec=_parse_int(
            env.get("CANDIDATE_EPISODE_TTL_SEC", "1800"),
            "CANDIDATE_EPISODE_TTL_SEC",
            min_value=1,
        ),
        candidate_context_require_1m_bar=_parse_bool(
            env.get("CANDIDATE_CONTEXT_REQUIRE_1M_BAR", "true")
        ),
        candidate_context_require_vwap=_parse_bool(
            env.get("CANDIDATE_CONTEXT_REQUIRE_VWAP", "false")
        ),
        candidate_max_active_per_code=_parse_int(
            env.get("CANDIDATE_MAX_ACTIVE_PER_CODE", "1"),
            "CANDIDATE_MAX_ACTIVE_PER_CODE",
            min_value=1,
        ),
        candidate_theme_source_states=_parse_csv_list(
            env.get("CANDIDATE_THEME_SOURCE_STATES", "LEADING,SPREADING"),
            "CANDIDATE_THEME_SOURCE_STATES",
        ),
        candidate_theme_member_roles=_parse_csv_list(
            env.get(
                "CANDIDATE_THEME_MEMBER_ROLES",
                "LEADER_CANDIDATE,CO_LEADER_CANDIDATE,FOLLOWER_CANDIDATE",
            ),
            "CANDIDATE_THEME_MEMBER_ROLES",
        ),
        candidate_condition_action_enter=env.get("CANDIDATE_CONDITION_ACTION_ENTER", "ENTER"),
        candidate_condition_action_exit=env.get("CANDIDATE_CONDITION_ACTION_EXIT", "EXIT"),
        strategy_engine_enabled=_parse_bool(env.get("STRATEGY_ENGINE_ENABLED", "true")),
        strategy_engine_observe_only=_parse_bool(
            env.get("STRATEGY_ENGINE_OBSERVE_ONLY", "true")
        ),
        strategy_engine_max_candidates=_parse_int(
            env.get("STRATEGY_ENGINE_MAX_CANDIDATES", "500"),
            "STRATEGY_ENGINE_MAX_CANDIDATES",
            min_value=1,
        ),
        strategy_engine_require_context_ready=_parse_bool(
            env.get("STRATEGY_ENGINE_REQUIRE_CONTEXT_READY", "true")
        ),
        strategy_engine_allowed_candidate_states=_parse_csv_list(
            env.get("STRATEGY_ENGINE_ALLOWED_CANDIDATE_STATES", "CONTEXT_READY,WATCHING"),
            "STRATEGY_ENGINE_ALLOWED_CANDIDATE_STATES",
        ),
        strategy_engine_stale_tick_sec=_parse_int(
            env.get("STRATEGY_ENGINE_STALE_TICK_SEC", "30"),
            "STRATEGY_ENGINE_STALE_TICK_SEC",
            min_value=1,
        ),
        strategy_engine_allowed_theme_states=_parse_csv_list(
            env.get("STRATEGY_ENGINE_ALLOWED_THEME_STATES", "LEADING,SPREADING"),
            "STRATEGY_ENGINE_ALLOWED_THEME_STATES",
        ),
        strategy_engine_allowed_theme_roles=_parse_csv_list(
            env.get(
                "STRATEGY_ENGINE_ALLOWED_THEME_ROLES",
                "LEADER_CANDIDATE,CO_LEADER_CANDIDATE,FOLLOWER_CANDIDATE",
            ),
            "STRATEGY_ENGINE_ALLOWED_THEME_ROLES",
        ),
        strategy_engine_require_1m_bar=_parse_bool(
            env.get("STRATEGY_ENGINE_REQUIRE_1M_BAR", "true")
        ),
        strategy_engine_require_vwap=_parse_bool(
            env.get("STRATEGY_ENGINE_REQUIRE_VWAP", "false")
        ),
        strategy_pullback_min_pct=_parse_float(
            env.get("STRATEGY_PULLBACK_MIN_PCT", "0.3"),
            "STRATEGY_PULLBACK_MIN_PCT",
            min_value=0.0,
        ),
        strategy_pullback_max_pct=_parse_float(
            env.get("STRATEGY_PULLBACK_MAX_PCT", "5.0"),
            "STRATEGY_PULLBACK_MAX_PCT",
            min_value=0.0,
        ),
        strategy_vwap_reclaim_tolerance_pct=_parse_float(
            env.get("STRATEGY_VWAP_RECLAIM_TOLERANCE_PCT", "1.0"),
            "STRATEGY_VWAP_RECLAIM_TOLERANCE_PCT",
            min_value=0.0,
        ),
        strategy_min_trade_value_delta_1m=_parse_float(
            env.get("STRATEGY_MIN_TRADE_VALUE_DELTA_1M", "0"),
            "STRATEGY_MIN_TRADE_VALUE_DELTA_1M",
            min_value=0.0,
        ),
        strategy_min_trade_value_delta_3m=_parse_float(
            env.get("STRATEGY_MIN_TRADE_VALUE_DELTA_3M", "0"),
            "STRATEGY_MIN_TRADE_VALUE_DELTA_3M",
            min_value=0.0,
        ),
        strategy_breakout_retest_near_high_pct=_parse_float(
            env.get("STRATEGY_BREAKOUT_RETEST_NEAR_HIGH_PCT", "2.0"),
            "STRATEGY_BREAKOUT_RETEST_NEAR_HIGH_PCT",
            min_value=0.0,
        ),
        strategy_follower_expansion_min_theme_rising_ratio=_parse_float(
            env.get("STRATEGY_FOLLOWER_EXPANSION_MIN_THEME_RISING_RATIO", "0.35"),
            "STRATEGY_FOLLOWER_EXPANSION_MIN_THEME_RISING_RATIO",
        ),
        strategy_config_version=env.get("STRATEGY_CONFIG_VERSION", "observe_v1"),
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


def _parse_float(value: str, field_name: str, *, min_value: float | None = None) -> float:
    normalized = value.strip()
    try:
        parsed = float(normalized)
    except ValueError as exc:
        raise ValueError(f"Unsupported float value for {field_name}: {value!r}") from exc

    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field_name} must be >= {min_value}")
    return parsed


def _validate_ratio(value: float, field_name: str) -> None:
    if value < 0 or value > 1:
        raise ValueError(f"{field_name} must be a ratio between 0 and 1")


def _validate_timezone(value: str) -> None:
    candidate_timezone(value)


def candidate_timezone(value: str) -> tzinfo:
    normalized = _require_non_empty_config(value)
    try:
        return ZoneInfo(normalized)
    except ZoneInfoNotFoundError as exc:
        if normalized == "Asia/Seoul":
            return timezone(timedelta(hours=9), name="Asia/Seoul")
        raise ValueError(
            f"Unsupported timezone for CANDIDATE_TRADE_DATE_TIMEZONE: {value!r}"
        ) from exc


def _require_non_empty_config(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("configuration value must not be empty")
    return normalized


def _normalize_non_empty(value: str) -> str:
    return _require_non_empty_config(value).upper()


def _normalize_list_values(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(_normalize_non_empty(value) for value in values)
    if len(set(normalized)) != len(normalized):
        raise ValueError("configuration list values must not contain duplicates")
    return normalized


def _parse_csv_list(value: str, field_name: str) -> tuple[str, ...]:
    parts = tuple(part.strip() for part in value.split(","))
    if any(part == "" for part in parts):
        raise ValueError(f"{field_name} must be a comma-separated non-empty list")
    return _normalize_list_values(parts)


def _parse_intervals(value: str) -> tuple[int, ...]:
    parts = [part.strip() for part in value.split(",")]
    if any(part == "" for part in parts):
        raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC must be a comma-separated integer list")
    try:
        intervals = [int(part) for part in parts]
    except ValueError as exc:
        raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC must contain integers") from exc
    return normalize_interval_list(intervals)
