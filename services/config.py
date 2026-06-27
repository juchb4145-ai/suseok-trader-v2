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
    ai_sidecar_openai_api_key_env: str = "OPENAI_API_KEY"
    ai_sidecar_openai_base_url: str = ""
    ai_sidecar_use_responses_api: bool = True
    ai_sidecar_structured_outputs_enabled: bool = True
    ai_sidecar_strict_schema: bool = True
    ai_sidecar_tools_enabled: bool = False
    ai_sidecar_order_tools_enabled: bool = False
    ai_sidecar_max_output_chars: int = 6000
    ai_sidecar_max_retries: int = 1
    ai_sidecar_store_raw_response: bool = False
    ai_sidecar_allow_manual_run: bool = True
    ai_sidecar_request_retention_days: int = 30
    ai_sidecar_default_operator_action: str = "REVIEW_ONLY"
    ai_sidecar_context_builder_enabled: bool = True
    ai_sidecar_context_default_limit: int = 50
    ai_sidecar_context_max_limit: int = 200
    ai_sidecar_context_persist_preview: bool = False
    ai_sidecar_context_schema_version: str = "ai-sidecar-context.v1"
    ai_sidecar_context_redact_paths: bool = True
    ai_sidecar_context_redact_secrets: bool = True
    ai_sidecar_context_include_raw_payload: bool = False
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
    risk_gate_enabled: bool = True
    risk_gate_observe_only: bool = True
    risk_gate_max_strategy_observations: int = 500
    risk_gate_require_strategy_matched: bool = True
    risk_gate_stale_tick_sec: int = 30
    risk_gate_strategy_stale_sec: int = 300
    risk_gate_max_spread_ticks: int = 5
    risk_gate_min_trade_value_delta_1m: float = 0.0
    risk_gate_min_cumulative_trade_value: float = 0.0
    risk_gate_min_execution_strength: float = 0.0
    risk_gate_max_change_rate: float = 25.0
    risk_gate_max_vwap_extension_pct: float = 8.0
    risk_gate_near_day_high_pct: float = 1.0
    risk_gate_min_theme_fresh_coverage_ratio: float = 0.3
    risk_gate_min_theme_rising_ratio: float = 0.35
    risk_gate_duplicate_active_candidate_limit: int = 1
    risk_gate_observation_cooldown_sec: int = 60
    risk_gate_config_version: str = "observe_v1"
    dry_run_oms_enabled: bool = False
    dry_run_intent_creation_enabled: bool = False
    dry_run_simulated_fill_enabled: bool = False
    dry_run_require_safety_gate: bool = True
    dry_run_require_strategy_matched: bool = True
    dry_run_require_risk_observe_pass: bool = True
    dry_run_require_candidate_context_ready: bool = True
    dry_run_max_daily_intents: int = 20
    dry_run_max_active_positions: int = 5
    dry_run_max_position_notional: float = 1_000_000
    dry_run_default_position_notional: float = 1_000_000
    dry_run_min_quantity: int = 1
    dry_run_intent_ttl_sec: int = 300
    dry_run_duplicate_cooldown_sec: int = 300
    dry_run_stale_tick_sec: int = 30
    dry_run_commission_rate: float = 0.0
    dry_run_tax_rate: float = 0.0
    dry_run_allow_sell: bool = False
    dry_run_allow_short: bool = False
    dry_run_allow_market_sim: bool = True
    dry_run_order_routing_enabled: bool = False
    dry_run_gateway_command_enabled: bool = False
    dry_run_allow_without_safety_draft_for_tests: bool = False
    dry_run_exit_engine_enabled: bool = False
    dry_run_exit_intent_creation_enabled: bool = False
    dry_run_exit_order_creation_enabled: bool = False
    dry_run_exit_simulated_fill_enabled: bool = False
    dry_run_exit_require_safety_gate: bool = True
    dry_run_exit_stop_loss_pct: float = 2.0
    dry_run_exit_take_profit_pct: float = 5.0
    dry_run_exit_trailing_stop_pct: float = 3.0
    dry_run_exit_max_hold_sec: int = 1800
    dry_run_exit_stale_tick_sec: int = 30
    dry_run_exit_min_hold_sec: int = 0
    dry_run_exit_intent_ttl_sec: int = 300
    dry_run_exit_allow_sell_close_only: bool = True
    dry_run_exit_allow_short: bool = False
    dry_run_exit_order_routing_enabled: bool = False
    dry_run_exit_gateway_command_enabled: bool = False
    dry_run_exit_config_version: str = "exit_dry_run_v1"
    live_sim_enabled: bool = False
    live_sim_order_routing_enabled: bool = False
    live_sim_gateway_command_enabled: bool = False
    live_sim_account_id: str = ""
    live_sim_account_mode: str = "SIMULATION"
    live_sim_broker_env: str = "SIMULATION"
    live_sim_server_mode: str = "SIMULATION"
    live_sim_kill_switch: bool = True
    live_sim_max_order_notional: float = 100_000
    live_sim_max_daily_order_count: int = 3
    live_sim_max_daily_notional: float = 300_000
    live_sim_max_active_orders: int = 1
    live_sim_max_active_positions: int = 1
    live_sim_duplicate_cooldown_sec: int = 600
    live_sim_order_ttl_sec: int = 60
    live_sim_require_dry_run_evidence: bool = True
    live_sim_require_risk_observe_pass: bool = True
    live_sim_require_strategy_matched: bool = True
    live_sim_require_candidate_context_ready: bool = True
    live_sim_require_fresh_tick: bool = True
    live_sim_stale_tick_sec: int = 15
    live_sim_allow_buy: bool = True
    live_sim_allow_sell: bool = False
    live_sim_allow_exit_sell: bool = False
    live_sim_allow_market_order: bool = False
    live_sim_allow_limit_order: bool = True
    live_sim_default_order_type: str = "LIMIT"
    live_sim_default_hoga: str = "00"
    live_sim_price_offset_ticks: int = 0
    live_sim_config_version: str = "live_sim_v1"
    dashboard_enabled: bool = True
    dashboard_refresh_sec: int = 5
    dashboard_snapshot_default_limit: int = 50
    dashboard_max_limit: int = 200
    dashboard_show_raw_json: bool = True
    dashboard_route_enabled: bool = True

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
        for field_name in (
            "risk_gate_max_strategy_observations",
            "risk_gate_stale_tick_sec",
            "risk_gate_strategy_stale_sec",
            "risk_gate_max_spread_ticks",
            "risk_gate_duplicate_active_candidate_limit",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.risk_gate_observation_cooldown_sec < 0:
            raise ValueError("RISK_GATE_OBSERVATION_COOLDOWN_SEC must be >= 0")
        for field_name in (
            "risk_gate_min_trade_value_delta_1m",
            "risk_gate_min_cumulative_trade_value",
            "risk_gate_min_execution_strength",
            "risk_gate_max_change_rate",
            "risk_gate_max_vwap_extension_pct",
            "risk_gate_near_day_high_pct",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        _validate_ratio(
            self.risk_gate_min_theme_fresh_coverage_ratio,
            "RISK_GATE_MIN_THEME_FRESH_COVERAGE_RATIO",
        )
        _validate_ratio(
            self.risk_gate_min_theme_rising_ratio,
            "RISK_GATE_MIN_THEME_RISING_RATIO",
        )
        object.__setattr__(
            self,
            "risk_gate_config_version",
            _require_non_empty_config(self.risk_gate_config_version),
        )
        for field_name in (
            "dry_run_max_daily_intents",
            "dry_run_max_active_positions",
            "dry_run_min_quantity",
            "dry_run_intent_ttl_sec",
            "dry_run_duplicate_cooldown_sec",
            "dry_run_stale_tick_sec",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        for field_name in (
            "dry_run_max_position_notional",
            "dry_run_default_position_notional",
        ):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name.upper()} must be > 0")
        if self.dry_run_default_position_notional > self.dry_run_max_position_notional:
            raise ValueError(
                "DRY_RUN_DEFAULT_POSITION_NOTIONAL must be <= "
                "DRY_RUN_MAX_POSITION_NOTIONAL"
            )
        for field_name in ("dry_run_commission_rate", "dry_run_tax_rate"):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        if self.dry_run_order_routing_enabled:
            raise ValueError("DRY_RUN_ORDER_ROUTING_ENABLED must remain false in PR10")
        if self.dry_run_gateway_command_enabled:
            raise ValueError("DRY_RUN_GATEWAY_COMMAND_ENABLED must remain false in PR10")
        if self.dry_run_allow_short:
            raise ValueError("DRY_RUN_ALLOW_SHORT must remain false in PR10")
        for field_name in (
            "dry_run_exit_stop_loss_pct",
            "dry_run_exit_take_profit_pct",
            "dry_run_exit_trailing_stop_pct",
            "dry_run_exit_max_hold_sec",
            "dry_run_exit_stale_tick_sec",
            "dry_run_exit_intent_ttl_sec",
        ):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name.upper()} must be > 0")
        if self.dry_run_exit_min_hold_sec < 0:
            raise ValueError("DRY_RUN_EXIT_MIN_HOLD_SEC must be >= 0")
        if not self.dry_run_exit_allow_sell_close_only:
            raise ValueError("DRY_RUN_EXIT_ALLOW_SELL_CLOSE_ONLY must remain true in PR11")
        if self.dry_run_exit_order_routing_enabled:
            raise ValueError("DRY_RUN_EXIT_ORDER_ROUTING_ENABLED must remain false in PR11")
        if self.dry_run_exit_gateway_command_enabled:
            raise ValueError("DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED must remain false in PR11")
        if self.dry_run_exit_allow_short:
            raise ValueError("DRY_RUN_EXIT_ALLOW_SHORT must remain false in PR11")
        object.__setattr__(
            self,
            "dry_run_exit_config_version",
            _require_non_empty_config(self.dry_run_exit_config_version),
        )
        for field_name in (
            "live_sim_max_order_notional",
            "live_sim_max_daily_notional",
        ):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name.upper()} must be > 0")
        for field_name in (
            "live_sim_max_daily_order_count",
            "live_sim_max_active_orders",
            "live_sim_max_active_positions",
            "live_sim_duplicate_cooldown_sec",
            "live_sim_order_ttl_sec",
            "live_sim_stale_tick_sec",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.live_sim_max_daily_notional < self.live_sim_max_order_notional:
            raise ValueError(
                "LIVE_SIM_MAX_DAILY_NOTIONAL must be >= LIVE_SIM_MAX_ORDER_NOTIONAL"
            )
        if self.live_sim_price_offset_ticks < 0:
            raise ValueError("LIVE_SIM_PRICE_OFFSET_TICKS must be >= 0")
        object.__setattr__(
            self,
            "live_sim_account_mode",
            _normalize_non_empty(self.live_sim_account_mode),
        )
        object.__setattr__(
            self,
            "live_sim_broker_env",
            _normalize_non_empty(self.live_sim_broker_env),
        )
        object.__setattr__(
            self,
            "live_sim_server_mode",
            _normalize_non_empty(self.live_sim_server_mode),
        )
        object.__setattr__(
            self,
            "live_sim_default_order_type",
            _normalize_non_empty(self.live_sim_default_order_type),
        )
        if self.live_sim_default_order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("LIVE_SIM_DEFAULT_ORDER_TYPE must be LIMIT or MARKET")
        if self.live_sim_default_order_type == "MARKET" and not self.live_sim_allow_market_order:
            raise ValueError(
                "LIVE_SIM_DEFAULT_ORDER_TYPE cannot be MARKET when "
                "LIVE_SIM_ALLOW_MARKET_ORDER is false"
            )
        if self.live_sim_default_order_type == "LIMIT" and not self.live_sim_allow_limit_order:
            raise ValueError(
                "LIVE_SIM_DEFAULT_ORDER_TYPE cannot be LIMIT when "
                "LIVE_SIM_ALLOW_LIMIT_ORDER is false"
            )
        object.__setattr__(
            self,
            "live_sim_default_hoga",
            _require_non_empty_config(self.live_sim_default_hoga),
        )
        object.__setattr__(
            self,
            "live_sim_config_version",
            _require_non_empty_config(self.live_sim_config_version),
        )
        for field_name in ("dashboard_refresh_sec", "dashboard_snapshot_default_limit"):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.dashboard_max_limit < 1:
            raise ValueError("DASHBOARD_MAX_LIMIT must be >= 1")
        if self.dashboard_snapshot_default_limit > self.dashboard_max_limit:
            raise ValueError(
                "DASHBOARD_SNAPSHOT_DEFAULT_LIMIT must be <= DASHBOARD_MAX_LIMIT"
            )
        for field_name in ("ai_sidecar_context_default_limit", "ai_sidecar_context_max_limit"):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.ai_sidecar_context_default_limit > self.ai_sidecar_context_max_limit:
            raise ValueError(
                "AI_SIDECAR_CONTEXT_DEFAULT_LIMIT must be <= AI_SIDECAR_CONTEXT_MAX_LIMIT"
            )
        if self.ai_sidecar_request_timeout_sec < 1:
            raise ValueError("AI_SIDECAR_REQUEST_TIMEOUT_SEC must be >= 1")
        if self.ai_sidecar_max_output_chars < 1:
            raise ValueError("AI_SIDECAR_MAX_OUTPUT_CHARS must be >= 1")
        if self.ai_sidecar_max_retries < 0 or self.ai_sidecar_max_retries > 3:
            raise ValueError("AI_SIDECAR_MAX_RETRIES must be between 0 and 3")
        if self.ai_sidecar_request_retention_days < 1:
            raise ValueError("AI_SIDECAR_REQUEST_RETENTION_DAYS must be >= 1")
        if self.ai_sidecar_tools_enabled:
            raise ValueError("AI_SIDECAR_TOOLS_ENABLED must remain false in PR AI-2")
        if self.ai_sidecar_order_tools_enabled:
            raise ValueError("AI_SIDECAR_ORDER_TOOLS_ENABLED must remain false in PR AI-2")
        object.__setattr__(
            self,
            "ai_sidecar_openai_api_key_env",
            _require_non_empty_config(self.ai_sidecar_openai_api_key_env),
        )
        object.__setattr__(
            self,
            "ai_sidecar_default_operator_action",
            _normalize_non_empty(self.ai_sidecar_default_operator_action),
        )
        object.__setattr__(
            self,
            "ai_sidecar_context_schema_version",
            _require_non_empty_config(self.ai_sidecar_context_schema_version),
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
        ai_sidecar_openai_api_key_env=env.get(
            "AI_SIDECAR_OPENAI_API_KEY_ENV",
            "OPENAI_API_KEY",
        ),
        ai_sidecar_openai_base_url=env.get("AI_SIDECAR_OPENAI_BASE_URL", ""),
        ai_sidecar_use_responses_api=_parse_bool(
            env.get("AI_SIDECAR_USE_RESPONSES_API", "true")
        ),
        ai_sidecar_structured_outputs_enabled=_parse_bool(
            env.get("AI_SIDECAR_STRUCTURED_OUTPUTS_ENABLED", "true")
        ),
        ai_sidecar_strict_schema=_parse_bool(env.get("AI_SIDECAR_STRICT_SCHEMA", "true")),
        ai_sidecar_tools_enabled=_parse_bool(env.get("AI_SIDECAR_TOOLS_ENABLED", "false")),
        ai_sidecar_order_tools_enabled=_parse_bool(
            env.get("AI_SIDECAR_ORDER_TOOLS_ENABLED", "false")
        ),
        ai_sidecar_max_output_chars=_parse_int(
            env.get("AI_SIDECAR_MAX_OUTPUT_CHARS", "6000"),
            "AI_SIDECAR_MAX_OUTPUT_CHARS",
            min_value=1,
        ),
        ai_sidecar_max_retries=_parse_int(
            env.get("AI_SIDECAR_MAX_RETRIES", "1"),
            "AI_SIDECAR_MAX_RETRIES",
            min_value=0,
        ),
        ai_sidecar_store_raw_response=_parse_bool(
            env.get("AI_SIDECAR_STORE_RAW_RESPONSE", "false")
        ),
        ai_sidecar_allow_manual_run=_parse_bool(
            env.get("AI_SIDECAR_ALLOW_MANUAL_RUN", "true")
        ),
        ai_sidecar_request_retention_days=_parse_int(
            env.get("AI_SIDECAR_REQUEST_RETENTION_DAYS", "30"),
            "AI_SIDECAR_REQUEST_RETENTION_DAYS",
            min_value=1,
        ),
        ai_sidecar_default_operator_action=env.get(
            "AI_SIDECAR_DEFAULT_OPERATOR_ACTION",
            "REVIEW_ONLY",
        ),
        ai_sidecar_context_builder_enabled=_parse_bool(
            env.get("AI_SIDECAR_CONTEXT_BUILDER_ENABLED", "true")
        ),
        ai_sidecar_context_default_limit=_parse_int(
            env.get("AI_SIDECAR_CONTEXT_DEFAULT_LIMIT", "50"),
            "AI_SIDECAR_CONTEXT_DEFAULT_LIMIT",
            min_value=1,
        ),
        ai_sidecar_context_max_limit=_parse_int(
            env.get("AI_SIDECAR_CONTEXT_MAX_LIMIT", "200"),
            "AI_SIDECAR_CONTEXT_MAX_LIMIT",
            min_value=1,
        ),
        ai_sidecar_context_persist_preview=_parse_bool(
            env.get("AI_SIDECAR_CONTEXT_PERSIST_PREVIEW", "false")
        ),
        ai_sidecar_context_schema_version=env.get(
            "AI_SIDECAR_CONTEXT_SCHEMA_VERSION",
            "ai-sidecar-context.v1",
        ),
        ai_sidecar_context_redact_paths=_parse_bool(
            env.get("AI_SIDECAR_CONTEXT_REDACT_PATHS", "true")
        ),
        ai_sidecar_context_redact_secrets=_parse_bool(
            env.get("AI_SIDECAR_CONTEXT_REDACT_SECRETS", "true")
        ),
        ai_sidecar_context_include_raw_payload=_parse_bool(
            env.get("AI_SIDECAR_CONTEXT_INCLUDE_RAW_PAYLOAD", "false")
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
        risk_gate_enabled=_parse_bool(env.get("RISK_GATE_ENABLED", "true")),
        risk_gate_observe_only=_parse_bool(env.get("RISK_GATE_OBSERVE_ONLY", "true")),
        risk_gate_max_strategy_observations=_parse_int(
            env.get("RISK_GATE_MAX_STRATEGY_OBSERVATIONS", "500"),
            "RISK_GATE_MAX_STRATEGY_OBSERVATIONS",
            min_value=1,
        ),
        risk_gate_require_strategy_matched=_parse_bool(
            env.get("RISK_GATE_REQUIRE_STRATEGY_MATCHED", "true")
        ),
        risk_gate_stale_tick_sec=_parse_int(
            env.get("RISK_GATE_STALE_TICK_SEC", "30"),
            "RISK_GATE_STALE_TICK_SEC",
            min_value=1,
        ),
        risk_gate_strategy_stale_sec=_parse_int(
            env.get("RISK_GATE_STRATEGY_STALE_SEC", "300"),
            "RISK_GATE_STRATEGY_STALE_SEC",
            min_value=1,
        ),
        risk_gate_max_spread_ticks=_parse_int(
            env.get("RISK_GATE_MAX_SPREAD_TICKS", "5"),
            "RISK_GATE_MAX_SPREAD_TICKS",
            min_value=1,
        ),
        risk_gate_min_trade_value_delta_1m=_parse_float(
            env.get("RISK_GATE_MIN_TRADE_VALUE_DELTA_1M", "0"),
            "RISK_GATE_MIN_TRADE_VALUE_DELTA_1M",
            min_value=0.0,
        ),
        risk_gate_min_cumulative_trade_value=_parse_float(
            env.get("RISK_GATE_MIN_CUMULATIVE_TRADE_VALUE", "0"),
            "RISK_GATE_MIN_CUMULATIVE_TRADE_VALUE",
            min_value=0.0,
        ),
        risk_gate_min_execution_strength=_parse_float(
            env.get("RISK_GATE_MIN_EXECUTION_STRENGTH", "0"),
            "RISK_GATE_MIN_EXECUTION_STRENGTH",
            min_value=0.0,
        ),
        risk_gate_max_change_rate=_parse_float(
            env.get("RISK_GATE_MAX_CHANGE_RATE", "25.0"),
            "RISK_GATE_MAX_CHANGE_RATE",
            min_value=0.0,
        ),
        risk_gate_max_vwap_extension_pct=_parse_float(
            env.get("RISK_GATE_MAX_VWAP_EXTENSION_PCT", "8.0"),
            "RISK_GATE_MAX_VWAP_EXTENSION_PCT",
            min_value=0.0,
        ),
        risk_gate_near_day_high_pct=_parse_float(
            env.get("RISK_GATE_NEAR_DAY_HIGH_PCT", "1.0"),
            "RISK_GATE_NEAR_DAY_HIGH_PCT",
            min_value=0.0,
        ),
        risk_gate_min_theme_fresh_coverage_ratio=_parse_float(
            env.get("RISK_GATE_MIN_THEME_FRESH_COVERAGE_RATIO", "0.3"),
            "RISK_GATE_MIN_THEME_FRESH_COVERAGE_RATIO",
        ),
        risk_gate_min_theme_rising_ratio=_parse_float(
            env.get("RISK_GATE_MIN_THEME_RISING_RATIO", "0.35"),
            "RISK_GATE_MIN_THEME_RISING_RATIO",
        ),
        risk_gate_duplicate_active_candidate_limit=_parse_int(
            env.get("RISK_GATE_DUPLICATE_ACTIVE_CANDIDATE_LIMIT", "1"),
            "RISK_GATE_DUPLICATE_ACTIVE_CANDIDATE_LIMIT",
            min_value=1,
        ),
        risk_gate_observation_cooldown_sec=_parse_int(
            env.get("RISK_GATE_OBSERVATION_COOLDOWN_SEC", "60"),
            "RISK_GATE_OBSERVATION_COOLDOWN_SEC",
            min_value=0,
        ),
        risk_gate_config_version=env.get("RISK_GATE_CONFIG_VERSION", "observe_v1"),
        dry_run_oms_enabled=_parse_bool(env.get("DRY_RUN_OMS_ENABLED", "false")),
        dry_run_intent_creation_enabled=_parse_bool(
            env.get("DRY_RUN_INTENT_CREATION_ENABLED", "false")
        ),
        dry_run_simulated_fill_enabled=_parse_bool(
            env.get("DRY_RUN_SIMULATED_FILL_ENABLED", "false")
        ),
        dry_run_require_safety_gate=_parse_bool(
            env.get("DRY_RUN_REQUIRE_SAFETY_GATE", "true")
        ),
        dry_run_require_strategy_matched=_parse_bool(
            env.get("DRY_RUN_REQUIRE_STRATEGY_MATCHED", "true")
        ),
        dry_run_require_risk_observe_pass=_parse_bool(
            env.get("DRY_RUN_REQUIRE_RISK_OBSERVE_PASS", "true")
        ),
        dry_run_require_candidate_context_ready=_parse_bool(
            env.get("DRY_RUN_REQUIRE_CANDIDATE_CONTEXT_READY", "true")
        ),
        dry_run_max_daily_intents=_parse_int(
            env.get("DRY_RUN_MAX_DAILY_INTENTS", "20"),
            "DRY_RUN_MAX_DAILY_INTENTS",
            min_value=1,
        ),
        dry_run_max_active_positions=_parse_int(
            env.get("DRY_RUN_MAX_ACTIVE_POSITIONS", "5"),
            "DRY_RUN_MAX_ACTIVE_POSITIONS",
            min_value=1,
        ),
        dry_run_max_position_notional=_parse_float(
            env.get("DRY_RUN_MAX_POSITION_NOTIONAL", "1000000"),
            "DRY_RUN_MAX_POSITION_NOTIONAL",
            min_value=0.0,
        ),
        dry_run_default_position_notional=_parse_float(
            env.get("DRY_RUN_DEFAULT_POSITION_NOTIONAL", "1000000"),
            "DRY_RUN_DEFAULT_POSITION_NOTIONAL",
            min_value=0.0,
        ),
        dry_run_min_quantity=_parse_int(
            env.get("DRY_RUN_MIN_QUANTITY", "1"),
            "DRY_RUN_MIN_QUANTITY",
            min_value=1,
        ),
        dry_run_intent_ttl_sec=_parse_int(
            env.get("DRY_RUN_INTENT_TTL_SEC", "300"),
            "DRY_RUN_INTENT_TTL_SEC",
            min_value=1,
        ),
        dry_run_duplicate_cooldown_sec=_parse_int(
            env.get("DRY_RUN_DUPLICATE_COOLDOWN_SEC", "300"),
            "DRY_RUN_DUPLICATE_COOLDOWN_SEC",
            min_value=1,
        ),
        dry_run_stale_tick_sec=_parse_int(
            env.get("DRY_RUN_STALE_TICK_SEC", "30"),
            "DRY_RUN_STALE_TICK_SEC",
            min_value=1,
        ),
        dry_run_commission_rate=_parse_float(
            env.get("DRY_RUN_COMMISSION_RATE", "0"),
            "DRY_RUN_COMMISSION_RATE",
            min_value=0.0,
        ),
        dry_run_tax_rate=_parse_float(
            env.get("DRY_RUN_TAX_RATE", "0"),
            "DRY_RUN_TAX_RATE",
            min_value=0.0,
        ),
        dry_run_allow_sell=_parse_bool(env.get("DRY_RUN_ALLOW_SELL", "false")),
        dry_run_allow_short=_parse_bool(env.get("DRY_RUN_ALLOW_SHORT", "false")),
        dry_run_allow_market_sim=_parse_bool(env.get("DRY_RUN_ALLOW_MARKET_SIM", "true")),
        dry_run_order_routing_enabled=_parse_bool(
            env.get("DRY_RUN_ORDER_ROUTING_ENABLED", "false")
        ),
        dry_run_gateway_command_enabled=_parse_bool(
            env.get("DRY_RUN_GATEWAY_COMMAND_ENABLED", "false")
        ),
        dry_run_allow_without_safety_draft_for_tests=_parse_bool(
            env.get("DRY_RUN_ALLOW_WITHOUT_SAFETY_DRAFT_FOR_TESTS", "false")
        ),
        dry_run_exit_engine_enabled=_parse_bool(
            env.get("DRY_RUN_EXIT_ENGINE_ENABLED", "false")
        ),
        dry_run_exit_intent_creation_enabled=_parse_bool(
            env.get("DRY_RUN_EXIT_INTENT_CREATION_ENABLED", "false")
        ),
        dry_run_exit_order_creation_enabled=_parse_bool(
            env.get("DRY_RUN_EXIT_ORDER_CREATION_ENABLED", "false")
        ),
        dry_run_exit_simulated_fill_enabled=_parse_bool(
            env.get("DRY_RUN_EXIT_SIMULATED_FILL_ENABLED", "false")
        ),
        dry_run_exit_require_safety_gate=_parse_bool(
            env.get("DRY_RUN_EXIT_REQUIRE_SAFETY_GATE", "true")
        ),
        dry_run_exit_stop_loss_pct=_parse_float(
            env.get("DRY_RUN_EXIT_STOP_LOSS_PCT", "2.0"),
            "DRY_RUN_EXIT_STOP_LOSS_PCT",
            min_value=0.0,
        ),
        dry_run_exit_take_profit_pct=_parse_float(
            env.get("DRY_RUN_EXIT_TAKE_PROFIT_PCT", "5.0"),
            "DRY_RUN_EXIT_TAKE_PROFIT_PCT",
            min_value=0.0,
        ),
        dry_run_exit_trailing_stop_pct=_parse_float(
            env.get("DRY_RUN_EXIT_TRAILING_STOP_PCT", "3.0"),
            "DRY_RUN_EXIT_TRAILING_STOP_PCT",
            min_value=0.0,
        ),
        dry_run_exit_max_hold_sec=_parse_int(
            env.get("DRY_RUN_EXIT_MAX_HOLD_SEC", "1800"),
            "DRY_RUN_EXIT_MAX_HOLD_SEC",
            min_value=1,
        ),
        dry_run_exit_stale_tick_sec=_parse_int(
            env.get("DRY_RUN_EXIT_STALE_TICK_SEC", "30"),
            "DRY_RUN_EXIT_STALE_TICK_SEC",
            min_value=1,
        ),
        dry_run_exit_min_hold_sec=_parse_int(
            env.get("DRY_RUN_EXIT_MIN_HOLD_SEC", "0"),
            "DRY_RUN_EXIT_MIN_HOLD_SEC",
            min_value=0,
        ),
        dry_run_exit_intent_ttl_sec=_parse_int(
            env.get("DRY_RUN_EXIT_INTENT_TTL_SEC", "300"),
            "DRY_RUN_EXIT_INTENT_TTL_SEC",
            min_value=1,
        ),
        dry_run_exit_allow_sell_close_only=_parse_bool(
            env.get("DRY_RUN_EXIT_ALLOW_SELL_CLOSE_ONLY", "true")
        ),
        dry_run_exit_allow_short=_parse_bool(env.get("DRY_RUN_EXIT_ALLOW_SHORT", "false")),
        dry_run_exit_order_routing_enabled=_parse_bool(
            env.get("DRY_RUN_EXIT_ORDER_ROUTING_ENABLED", "false")
        ),
        dry_run_exit_gateway_command_enabled=_parse_bool(
            env.get("DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED", "false")
        ),
        dry_run_exit_config_version=env.get(
            "DRY_RUN_EXIT_CONFIG_VERSION",
            "exit_dry_run_v1",
        ),
        live_sim_enabled=_parse_bool(env.get("LIVE_SIM_ENABLED", "false")),
        live_sim_order_routing_enabled=_parse_bool(
            env.get("LIVE_SIM_ORDER_ROUTING_ENABLED", "false")
        ),
        live_sim_gateway_command_enabled=_parse_bool(
            env.get("LIVE_SIM_GATEWAY_COMMAND_ENABLED", "false")
        ),
        live_sim_account_id=env.get("LIVE_SIM_ACCOUNT_ID", ""),
        live_sim_account_mode=env.get("LIVE_SIM_ACCOUNT_MODE", "SIMULATION"),
        live_sim_broker_env=env.get("LIVE_SIM_BROKER_ENV", "SIMULATION"),
        live_sim_server_mode=env.get("LIVE_SIM_SERVER_MODE", "SIMULATION"),
        live_sim_kill_switch=_parse_bool(env.get("LIVE_SIM_KILL_SWITCH", "true")),
        live_sim_max_order_notional=_parse_float(
            env.get("LIVE_SIM_MAX_ORDER_NOTIONAL", "100000"),
            "LIVE_SIM_MAX_ORDER_NOTIONAL",
            min_value=0.0,
        ),
        live_sim_max_daily_order_count=_parse_int(
            env.get("LIVE_SIM_MAX_DAILY_ORDER_COUNT", "3"),
            "LIVE_SIM_MAX_DAILY_ORDER_COUNT",
            min_value=1,
        ),
        live_sim_max_daily_notional=_parse_float(
            env.get("LIVE_SIM_MAX_DAILY_NOTIONAL", "300000"),
            "LIVE_SIM_MAX_DAILY_NOTIONAL",
            min_value=0.0,
        ),
        live_sim_max_active_orders=_parse_int(
            env.get("LIVE_SIM_MAX_ACTIVE_ORDERS", "1"),
            "LIVE_SIM_MAX_ACTIVE_ORDERS",
            min_value=1,
        ),
        live_sim_max_active_positions=_parse_int(
            env.get("LIVE_SIM_MAX_ACTIVE_POSITIONS", "1"),
            "LIVE_SIM_MAX_ACTIVE_POSITIONS",
            min_value=1,
        ),
        live_sim_duplicate_cooldown_sec=_parse_int(
            env.get("LIVE_SIM_DUPLICATE_COOLDOWN_SEC", "600"),
            "LIVE_SIM_DUPLICATE_COOLDOWN_SEC",
            min_value=1,
        ),
        live_sim_order_ttl_sec=_parse_int(
            env.get("LIVE_SIM_ORDER_TTL_SEC", "60"),
            "LIVE_SIM_ORDER_TTL_SEC",
            min_value=1,
        ),
        live_sim_require_dry_run_evidence=_parse_bool(
            env.get("LIVE_SIM_REQUIRE_DRY_RUN_EVIDENCE", "true")
        ),
        live_sim_require_risk_observe_pass=_parse_bool(
            env.get("LIVE_SIM_REQUIRE_RISK_OBSERVE_PASS", "true")
        ),
        live_sim_require_strategy_matched=_parse_bool(
            env.get("LIVE_SIM_REQUIRE_STRATEGY_MATCHED", "true")
        ),
        live_sim_require_candidate_context_ready=_parse_bool(
            env.get("LIVE_SIM_REQUIRE_CANDIDATE_CONTEXT_READY", "true")
        ),
        live_sim_require_fresh_tick=_parse_bool(
            env.get("LIVE_SIM_REQUIRE_FRESH_TICK", "true")
        ),
        live_sim_stale_tick_sec=_parse_int(
            env.get("LIVE_SIM_STALE_TICK_SEC", "15"),
            "LIVE_SIM_STALE_TICK_SEC",
            min_value=1,
        ),
        live_sim_allow_buy=_parse_bool(env.get("LIVE_SIM_ALLOW_BUY", "true")),
        live_sim_allow_sell=_parse_bool(env.get("LIVE_SIM_ALLOW_SELL", "false")),
        live_sim_allow_exit_sell=_parse_bool(env.get("LIVE_SIM_ALLOW_EXIT_SELL", "false")),
        live_sim_allow_market_order=_parse_bool(
            env.get("LIVE_SIM_ALLOW_MARKET_ORDER", "false")
        ),
        live_sim_allow_limit_order=_parse_bool(env.get("LIVE_SIM_ALLOW_LIMIT_ORDER", "true")),
        live_sim_default_order_type=env.get("LIVE_SIM_DEFAULT_ORDER_TYPE", "LIMIT"),
        live_sim_default_hoga=env.get("LIVE_SIM_DEFAULT_HOGA", "00"),
        live_sim_price_offset_ticks=_parse_int(
            env.get("LIVE_SIM_PRICE_OFFSET_TICKS", "0"),
            "LIVE_SIM_PRICE_OFFSET_TICKS",
            min_value=0,
        ),
        live_sim_config_version=env.get("LIVE_SIM_CONFIG_VERSION", "live_sim_v1"),
        dashboard_enabled=_parse_bool(env.get("DASHBOARD_ENABLED", "true")),
        dashboard_refresh_sec=_parse_int(
            env.get("DASHBOARD_REFRESH_SEC", "5"),
            "DASHBOARD_REFRESH_SEC",
            min_value=1,
        ),
        dashboard_snapshot_default_limit=_parse_int(
            env.get("DASHBOARD_SNAPSHOT_DEFAULT_LIMIT", "50"),
            "DASHBOARD_SNAPSHOT_DEFAULT_LIMIT",
            min_value=1,
        ),
        dashboard_max_limit=_parse_int(
            env.get("DASHBOARD_MAX_LIMIT", "200"),
            "DASHBOARD_MAX_LIMIT",
            min_value=1,
        ),
        dashboard_show_raw_json=_parse_bool(env.get("DASHBOARD_SHOW_RAW_JSON", "true")),
        dashboard_route_enabled=_parse_bool(env.get("DASHBOARD_ROUTE_ENABLED", "true")),
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
