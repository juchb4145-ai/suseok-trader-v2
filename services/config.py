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


class TradingProfile(StrEnum):
    OBSERVE = "OBSERVE"
    LIVE_SIM_PILOT = "LIVE_SIM_PILOT"


@dataclass(frozen=True)
class Settings:
    trading_profile: TradingProfile = TradingProfile.OBSERVE
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
    ai_candidate_scorer_enabled: bool = False
    ai_candidate_scorer_provider: str = "mock"
    ai_candidate_scorer_model: str = ""
    ai_candidate_scorer_timeout_seconds: int = 10
    ai_candidate_scorer_max_candidates: int = 10
    ai_candidate_scorer_min_score: int = 70
    ai_candidate_scorer_min_confidence: int = 60
    ai_candidate_scorer_store_raw_response: bool = False
    ai_candidate_scorer_require_strict_json: bool = True
    ai_candidate_scorer_allow_order_actions: bool = False
    ai_candidate_scorer_fail_open: bool = True
    ai_candidate_scorer_attach_to_order_plan: bool = False
    ai_candidate_scorer_attach_to_live_sim_run: bool = False
    ai_candidate_scorer_max_prompt_chars: int = 12000
    ai_candidate_scorer_redact_account_id: bool = True
    ai_candidate_risk_reward_stop_loss_min: float = 1.0
    ai_candidate_risk_reward_stop_loss_max: float = 4.0
    ai_candidate_risk_reward_take_profit_min: float = 2.0
    ai_candidate_risk_reward_take_profit_max: float = 8.0
    ai_candidate_risk_reward_trailing_min: float = 1.0
    ai_candidate_risk_reward_trailing_max: float = 4.0
    ai_candidate_risk_reward_max_hold_min_sec: int = 300
    ai_candidate_risk_reward_max_hold_max_sec: int = 7200
    ai_external_llm_enabled: bool = False
    ai_external_llm_provider: str = "none"
    ai_external_llm_model: str = ""
    ai_external_llm_api_key_env: str = "OPENAI_API_KEY"
    ai_external_llm_base_url: str = ""
    ai_external_llm_timeout_seconds: int = 10
    ai_external_llm_max_retries: int = 1
    ai_external_llm_retry_backoff_seconds: float = 0.5
    ai_external_llm_max_response_chars: int = 8000
    ai_external_llm_temperature: float = 0.0
    ai_external_llm_require_json_schema: bool = True
    ai_external_llm_store_request: bool = False
    ai_external_llm_store_response: bool = False
    ai_external_llm_redact_prompt: bool = True
    ai_external_llm_fail_open: bool = True
    ai_external_llm_daily_call_limit: int = 100
    ai_external_llm_per_run_call_limit: int = 1
    ai_external_llm_cost_guard_enabled: bool = True
    ai_external_llm_allow_network: bool = False
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
    naver_theme_import_enabled: bool = False
    naver_theme_import_base_url: str = "https://finance.naver.com/sise/theme.naver"
    naver_theme_import_timeout_seconds: float = 10.0
    naver_theme_import_max_themes: int = 50
    naver_theme_import_request_sleep_seconds: float = 0.3
    naver_theme_import_replace: bool = False
    naver_theme_import_min_member_count: int = 2
    naver_theme_import_abort_on_empty: bool = True
    theme_leadership_enabled: bool = True
    theme_leadership_top_theme_count: int = 5
    theme_leadership_max_stocks_per_theme: int = 3
    theme_leadership_max_total_watchset: int = 20
    theme_leadership_min_valid_members: int = 2
    theme_leadership_min_fresh_coverage_ratio: float = 0.4
    theme_leadership_condition_boost_enabled: bool = True
    theme_leadership_write_candidate_sources: bool = False
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
    entry_timing_enabled: bool = True
    entry_timing_write_order_plan_drafts: bool = True
    entry_timing_max_plans_per_run: int = 20
    entry_timing_plan_ttl_seconds: int = 90
    entry_timing_pullback_min_pct: float = 1.0
    entry_timing_pullback_max_pct: float = 4.5
    entry_timing_vwap_reclaim_tolerance_pct: float = 0.7
    entry_timing_vwap_overextended_pct: float = 3.0
    entry_timing_chase_near_high_pct: float = 0.7
    entry_timing_max_spread_ticks: int = 3
    entry_timing_min_turnover_krw: float = 500_000_000
    entry_timing_min_execution_strength: float = 100.0
    entry_timing_default_notional: float = 100_000
    entry_timing_max_notional: float = 100_000
    entry_timing_allow_market_order: bool = False
    entry_timing_price_offset_ticks: int = 0
    entry_timing_allow_follower_in_spreading: bool = True
    entry_timing_allow_follower_in_leader_only: bool = False
    entry_timing_require_risk_observe_pass: bool = False
    entry_timing_require_strategy_matched: bool = False
    entry_timing_stale_max_seconds: int = 60
    entry_timing_config_version: str = "entry_timing_v1"
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
    live_sim_pilot_pipeline_enabled: bool = False
    live_sim_pilot_auto_queue_command: bool = False
    live_sim_order_plan_routing_enabled: bool = False
    live_sim_order_plan_require_plan_ready: bool = True
    live_sim_order_plan_require_fresh_tick: bool = True
    live_sim_order_plan_stale_sec: int = 30
    live_sim_order_plan_max_price_drift_pct: float = 0.8
    live_sim_order_plan_require_strategy_matched: bool = True
    live_sim_order_plan_require_risk_observe_pass: bool = True
    live_sim_order_plan_require_candidate_context_ready: bool = True
    live_sim_order_plan_require_dry_run_evidence: bool = False
    live_sim_order_plan_max_plans_per_run: int = 3
    live_sim_order_plan_max_commands_per_run: int = 1
    live_sim_order_plan_min_notional: float = 10_000
    live_sim_order_plan_default_notional: float = 100_000
    live_sim_order_plan_max_notional: float = 100_000
    live_sim_order_plan_allow_market_order: bool = False
    live_sim_order_plan_allowed_side: str = "BUY"
    live_sim_fee_rate: float = 0.0
    live_sim_tax_rate: float = 0.0
    live_sim_position_allow_scale_in: bool = False
    live_sim_position_max_per_code: int = 1
    live_sim_cancel_enabled: bool = False
    live_sim_cancel_unfilled_enabled: bool = False
    live_sim_cancel_order_ttl_sec: int = 60
    live_sim_cancel_max_commands_per_run: int = 3
    live_sim_cancel_require_broker_order_no: bool = True
    live_sim_cancel_allow_without_broker_order_no: bool = False
    live_sim_cancel_kill_switch: bool = False
    live_sim_exit_engine_enabled: bool = False
    live_sim_exit_order_creation_enabled: bool = False
    live_sim_exit_gateway_command_enabled: bool = False
    live_sim_exit_allow_sell_close_only: bool = True
    live_sim_exit_allow_short: bool = False
    live_sim_exit_default_order_type: str = "LIMIT"
    live_sim_exit_allow_market_order: bool = False
    live_sim_exit_use_market_for_stop: bool = False
    live_sim_exit_stop_loss_pct: float = 3.0
    live_sim_exit_take_profit_pct: float = 5.0
    live_sim_exit_trailing_stop_pct: float = 2.5
    live_sim_exit_trailing_activation_pct: float = 2.0
    live_sim_exit_max_hold_sec: int = 1800
    live_sim_exit_min_hold_sec: int = 30
    live_sim_exit_eod_flatten_enabled: bool = False
    live_sim_exit_eod_flatten_time: str = "15:15:00"
    live_sim_exit_max_commands_per_run: int = 3
    live_sim_exit_price_offset_ticks: int = 0
    live_sim_reconcile_enabled: bool = True
    live_sim_reconcile_request_broker_snapshot_enabled: bool = False
    live_sim_reconcile_block_new_buy_on_mismatch: bool = True
    live_sim_reconcile_allow_exit_on_mismatch: bool = True
    live_sim_reconcile_stale_order_sec: int = 300
    live_sim_operating_cycle_enabled: bool = True
    live_sim_operating_default_mode: str = "OBSERVE_CYCLE"
    live_sim_operating_max_buy_commands_per_cycle: int = 1
    live_sim_operating_max_cancel_commands_per_cycle: int = 3
    live_sim_operating_max_exit_commands_per_cycle: int = 3
    live_sim_operating_require_preflight_pass_for_queue: bool = True
    live_sim_operating_include_ai: bool = True
    live_sim_operating_include_no_buy: bool = True
    live_sim_operating_write_runs: bool = True
    no_buy_sentinel_enabled: bool = True
    no_buy_sentinel_market_open_time: str = "09:00:00"
    no_buy_sentinel_minutes_after_open: int = 20
    no_buy_sentinel_top_near_miss_limit: int = 10
    no_buy_sentinel_lookback_minutes: int = 60
    no_buy_sentinel_include_ai: bool = True
    no_buy_sentinel_include_config: bool = True
    no_buy_sentinel_include_reconcile: bool = True
    no_buy_sentinel_write_snapshots: bool = True
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
        object.__setattr__(
            self,
            "naver_theme_import_base_url",
            _require_non_empty_config(self.naver_theme_import_base_url),
        )
        if self.naver_theme_import_timeout_seconds <= 0:
            raise ValueError("NAVER_THEME_IMPORT_TIMEOUT_SECONDS must be > 0")
        if self.naver_theme_import_max_themes < 1:
            raise ValueError("NAVER_THEME_IMPORT_MAX_THEMES must be >= 1")
        if self.naver_theme_import_request_sleep_seconds < 0:
            raise ValueError("NAVER_THEME_IMPORT_REQUEST_SLEEP_SECONDS must be >= 0")
        if self.naver_theme_import_min_member_count < 1:
            raise ValueError("NAVER_THEME_IMPORT_MIN_MEMBER_COUNT must be >= 1")
        for field_name in (
            "theme_leadership_top_theme_count",
            "theme_leadership_max_stocks_per_theme",
            "theme_leadership_max_total_watchset",
            "theme_leadership_min_valid_members",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        _validate_ratio(
            self.theme_leadership_min_fresh_coverage_ratio,
            "THEME_LEADERSHIP_MIN_FRESH_COVERAGE_RATIO",
        )
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
            "entry_timing_max_plans_per_run",
            "entry_timing_plan_ttl_seconds",
            "entry_timing_max_spread_ticks",
            "entry_timing_stale_max_seconds",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        for field_name in (
            "entry_timing_pullback_min_pct",
            "entry_timing_pullback_max_pct",
            "entry_timing_vwap_reclaim_tolerance_pct",
            "entry_timing_vwap_overextended_pct",
            "entry_timing_chase_near_high_pct",
            "entry_timing_min_turnover_krw",
            "entry_timing_min_execution_strength",
            "entry_timing_default_notional",
            "entry_timing_max_notional",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        if self.entry_timing_pullback_max_pct < self.entry_timing_pullback_min_pct:
            raise ValueError(
                "ENTRY_TIMING_PULLBACK_MAX_PCT must be >= ENTRY_TIMING_PULLBACK_MIN_PCT"
            )
        if self.entry_timing_default_notional <= 0:
            raise ValueError("ENTRY_TIMING_DEFAULT_NOTIONAL must be > 0")
        if self.entry_timing_max_notional <= 0:
            raise ValueError("ENTRY_TIMING_MAX_NOTIONAL must be > 0")
        if self.entry_timing_default_notional > self.entry_timing_max_notional:
            raise ValueError(
                "ENTRY_TIMING_DEFAULT_NOTIONAL must be <= ENTRY_TIMING_MAX_NOTIONAL"
            )
        if self.entry_timing_allow_market_order:
            raise ValueError("ENTRY_TIMING_ALLOW_MARKET_ORDER must remain false in PR-3")
        if self.entry_timing_price_offset_ticks < 0:
            raise ValueError("ENTRY_TIMING_PRICE_OFFSET_TICKS must be >= 0")
        object.__setattr__(
            self,
            "entry_timing_config_version",
            _require_non_empty_config(self.entry_timing_config_version),
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
                "DRY_RUN_DEFAULT_POSITION_NOTIONAL must be <= " "DRY_RUN_MAX_POSITION_NOTIONAL"
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
            raise ValueError("LIVE_SIM_MAX_DAILY_NOTIONAL must be >= LIVE_SIM_MAX_ORDER_NOTIONAL")
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
        for field_name in (
            "live_sim_order_plan_stale_sec",
            "live_sim_order_plan_max_plans_per_run",
            "live_sim_order_plan_max_commands_per_run",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        for field_name in (
            "live_sim_order_plan_max_price_drift_pct",
            "live_sim_order_plan_min_notional",
            "live_sim_order_plan_default_notional",
            "live_sim_order_plan_max_notional",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        if self.live_sim_order_plan_min_notional > self.live_sim_order_plan_max_notional:
            raise ValueError(
                "LIVE_SIM_ORDER_PLAN_MIN_NOTIONAL must be <= "
                "LIVE_SIM_ORDER_PLAN_MAX_NOTIONAL"
            )
        if self.live_sim_order_plan_default_notional > self.live_sim_order_plan_max_notional:
            raise ValueError(
                "LIVE_SIM_ORDER_PLAN_DEFAULT_NOTIONAL must be <= "
                "LIVE_SIM_ORDER_PLAN_MAX_NOTIONAL"
            )
        if self.live_sim_order_plan_allow_market_order:
            raise ValueError("LIVE_SIM_ORDER_PLAN_ALLOW_MARKET_ORDER must remain false in PR-4")
        object.__setattr__(
            self,
            "live_sim_order_plan_allowed_side",
            _normalize_non_empty(self.live_sim_order_plan_allowed_side),
        )
        if self.live_sim_order_plan_allowed_side != "BUY":
            raise ValueError("LIVE_SIM_ORDER_PLAN_ALLOWED_SIDE must be BUY in PR-4")
        for field_name in ("live_sim_fee_rate", "live_sim_tax_rate"):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        if self.live_sim_position_max_per_code < 1:
            raise ValueError("LIVE_SIM_POSITION_MAX_PER_CODE must be >= 1")
        for field_name in (
            "live_sim_cancel_order_ttl_sec",
            "live_sim_cancel_max_commands_per_run",
            "live_sim_exit_max_hold_sec",
            "live_sim_exit_max_commands_per_run",
            "live_sim_reconcile_stale_order_sec",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.live_sim_exit_min_hold_sec < 0:
            raise ValueError("LIVE_SIM_EXIT_MIN_HOLD_SEC must be >= 0")
        for field_name in (
            "live_sim_exit_stop_loss_pct",
            "live_sim_exit_take_profit_pct",
            "live_sim_exit_trailing_stop_pct",
            "live_sim_exit_trailing_activation_pct",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        if not self.live_sim_exit_allow_sell_close_only:
            raise ValueError("LIVE_SIM_EXIT_ALLOW_SELL_CLOSE_ONLY must remain true")
        if self.live_sim_exit_allow_short:
            raise ValueError("LIVE_SIM_EXIT_ALLOW_SHORT must remain false")
        object.__setattr__(
            self,
            "live_sim_exit_default_order_type",
            _normalize_non_empty(self.live_sim_exit_default_order_type),
        )
        if self.live_sim_exit_default_order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("LIVE_SIM_EXIT_DEFAULT_ORDER_TYPE must be LIMIT or MARKET")
        if (
            self.live_sim_exit_default_order_type == "MARKET"
            and not self.live_sim_exit_allow_market_order
        ):
            raise ValueError(
                "LIVE_SIM_EXIT_DEFAULT_ORDER_TYPE cannot be MARKET when "
                "LIVE_SIM_EXIT_ALLOW_MARKET_ORDER is false"
            )
        if self.live_sim_exit_allow_market_order and not self.live_sim_allow_market_order:
            raise ValueError(
                "LIVE_SIM_EXIT_ALLOW_MARKET_ORDER requires LIVE_SIM_ALLOW_MARKET_ORDER"
            )
        if self.live_sim_exit_price_offset_ticks < 0:
            raise ValueError("LIVE_SIM_EXIT_PRICE_OFFSET_TICKS must be >= 0")
        if (
            self.live_sim_cancel_allow_without_broker_order_no
            and self.live_sim_cancel_require_broker_order_no
        ):
            raise ValueError(
                "LIVE_SIM_CANCEL_ALLOW_WITHOUT_BROKER_ORDER_NO requires "
                "LIVE_SIM_CANCEL_REQUIRE_BROKER_ORDER_NO=false"
            )
        object.__setattr__(
            self,
            "live_sim_operating_default_mode",
            _normalize_non_empty(self.live_sim_operating_default_mode),
        )
        if self.live_sim_operating_default_mode not in {
            "OBSERVE_CYCLE",
            "PILOT_BUY_ONLY",
            "PILOT_FULL_LIFECYCLE",
            "PROTECT_ONLY",
        }:
            raise ValueError(
                "LIVE_SIM_OPERATING_DEFAULT_MODE must be one of "
                "OBSERVE_CYCLE, PILOT_BUY_ONLY, PILOT_FULL_LIFECYCLE, PROTECT_ONLY"
            )
        for field_name in (
            "live_sim_operating_max_buy_commands_per_cycle",
            "live_sim_operating_max_cancel_commands_per_cycle",
            "live_sim_operating_max_exit_commands_per_cycle",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        for field_name in ("dashboard_refresh_sec", "dashboard_snapshot_default_limit"):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.dashboard_max_limit < 1:
            raise ValueError("DASHBOARD_MAX_LIMIT must be >= 1")
        if self.dashboard_snapshot_default_limit > self.dashboard_max_limit:
            raise ValueError("DASHBOARD_SNAPSHOT_DEFAULT_LIMIT must be <= DASHBOARD_MAX_LIMIT")
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
        object.__setattr__(
            self,
            "ai_candidate_scorer_provider",
            _normalize_non_empty(self.ai_candidate_scorer_provider).lower(),
        )
        if self.ai_candidate_scorer_timeout_seconds < 1:
            raise ValueError("AI_CANDIDATE_SCORER_TIMEOUT_SECONDS must be >= 1")
        if self.ai_candidate_scorer_max_candidates < 1:
            raise ValueError("AI_CANDIDATE_SCORER_MAX_CANDIDATES must be >= 1")
        for field_name in (
            "ai_candidate_scorer_min_score",
            "ai_candidate_scorer_min_confidence",
        ):
            value = getattr(self, field_name)
            if value < 0 or value > 100:
                raise ValueError(f"{field_name.upper()} must be between 0 and 100")
        if self.ai_candidate_scorer_allow_order_actions:
            raise ValueError("AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS must remain false")
        if not self.ai_candidate_scorer_fail_open:
            raise ValueError("AI_CANDIDATE_SCORER_FAIL_OPEN must remain true")
        if self.ai_candidate_scorer_max_prompt_chars < 1000:
            raise ValueError("AI_CANDIDATE_SCORER_MAX_PROMPT_CHARS must be >= 1000")
        if self.ai_candidate_risk_reward_stop_loss_min < 0:
            raise ValueError("AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MIN must be >= 0")
        if (
            self.ai_candidate_risk_reward_stop_loss_max
            < self.ai_candidate_risk_reward_stop_loss_min
        ):
            raise ValueError(
                "AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MAX must be >= "
                "AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MIN"
            )
        if (
            self.ai_candidate_risk_reward_take_profit_max
            < self.ai_candidate_risk_reward_take_profit_min
        ):
            raise ValueError(
                "AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MAX must be >= "
                "AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MIN"
            )
        if (
            self.ai_candidate_risk_reward_trailing_max
            < self.ai_candidate_risk_reward_trailing_min
        ):
            raise ValueError(
                "AI_CANDIDATE_RISK_REWARD_TRAILING_MAX must be >= "
                "AI_CANDIDATE_RISK_REWARD_TRAILING_MIN"
            )
        if self.ai_candidate_risk_reward_max_hold_min_sec < 1:
            raise ValueError("AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MIN_SEC must be >= 1")
        if (
            self.ai_candidate_risk_reward_max_hold_max_sec
            < self.ai_candidate_risk_reward_max_hold_min_sec
        ):
            raise ValueError(
                "AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MAX_SEC must be >= "
                "AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MIN_SEC"
            )
        object.__setattr__(
            self,
            "ai_external_llm_provider",
            _normalize_non_empty(self.ai_external_llm_provider).lower(),
        )
        object.__setattr__(
            self,
            "ai_external_llm_api_key_env",
            _require_non_empty_config(self.ai_external_llm_api_key_env),
        )
        if self.ai_external_llm_timeout_seconds < 1:
            raise ValueError("AI_EXTERNAL_LLM_TIMEOUT_SECONDS must be >= 1")
        if self.ai_external_llm_max_retries < 0 or self.ai_external_llm_max_retries > 3:
            raise ValueError("AI_EXTERNAL_LLM_MAX_RETRIES must be between 0 and 3")
        if self.ai_external_llm_retry_backoff_seconds < 0:
            raise ValueError("AI_EXTERNAL_LLM_RETRY_BACKOFF_SECONDS must be >= 0")
        if self.ai_external_llm_max_response_chars < 1:
            raise ValueError("AI_EXTERNAL_LLM_MAX_RESPONSE_CHARS must be >= 1")
        if self.ai_external_llm_temperature < 0 or self.ai_external_llm_temperature > 2:
            raise ValueError("AI_EXTERNAL_LLM_TEMPERATURE must be between 0 and 2")
        if not self.ai_external_llm_fail_open:
            raise ValueError("AI_EXTERNAL_LLM_FAIL_OPEN must remain true")
        if not self.ai_external_llm_redact_prompt:
            raise ValueError("AI_EXTERNAL_LLM_REDACT_PROMPT must remain true")
        if self.ai_external_llm_daily_call_limit < 0:
            raise ValueError("AI_EXTERNAL_LLM_DAILY_CALL_LIMIT must be >= 0")
        if self.ai_external_llm_per_run_call_limit < 0:
            raise ValueError("AI_EXTERNAL_LLM_PER_RUN_CALL_LIMIT must be >= 0")
        object.__setattr__(
            self,
            "no_buy_sentinel_market_open_time",
            _validate_time_string(
                self.no_buy_sentinel_market_open_time,
                "NO_BUY_SENTINEL_MARKET_OPEN_TIME",
            ),
        )
        for field_name in (
            "no_buy_sentinel_minutes_after_open",
            "no_buy_sentinel_top_near_miss_limit",
            "no_buy_sentinel_lookback_minutes",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")

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
        trading_profile=_parse_trading_profile(
            env.get("TRADING_PROFILE", TradingProfile.OBSERVE.value)
        ),
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
        ai_sidecar_use_responses_api=_parse_bool(env.get("AI_SIDECAR_USE_RESPONSES_API", "true")),
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
        ai_sidecar_allow_manual_run=_parse_bool(env.get("AI_SIDECAR_ALLOW_MANUAL_RUN", "true")),
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
        ai_candidate_scorer_enabled=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_ENABLED", "false")
        ),
        ai_candidate_scorer_provider=env.get("AI_CANDIDATE_SCORER_PROVIDER", "mock"),
        ai_candidate_scorer_model=env.get("AI_CANDIDATE_SCORER_MODEL", ""),
        ai_candidate_scorer_timeout_seconds=_parse_int(
            env.get("AI_CANDIDATE_SCORER_TIMEOUT_SECONDS", "10"),
            "AI_CANDIDATE_SCORER_TIMEOUT_SECONDS",
            min_value=1,
        ),
        ai_candidate_scorer_max_candidates=_parse_int(
            env.get("AI_CANDIDATE_SCORER_MAX_CANDIDATES", "10"),
            "AI_CANDIDATE_SCORER_MAX_CANDIDATES",
            min_value=1,
        ),
        ai_candidate_scorer_min_score=_parse_int(
            env.get("AI_CANDIDATE_SCORER_MIN_SCORE", "70"),
            "AI_CANDIDATE_SCORER_MIN_SCORE",
            min_value=0,
        ),
        ai_candidate_scorer_min_confidence=_parse_int(
            env.get("AI_CANDIDATE_SCORER_MIN_CONFIDENCE", "60"),
            "AI_CANDIDATE_SCORER_MIN_CONFIDENCE",
            min_value=0,
        ),
        ai_candidate_scorer_store_raw_response=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_STORE_RAW_RESPONSE", "false")
        ),
        ai_candidate_scorer_require_strict_json=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_REQUIRE_STRICT_JSON", "true")
        ),
        ai_candidate_scorer_allow_order_actions=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS", "false")
        ),
        ai_candidate_scorer_fail_open=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_FAIL_OPEN", "true")
        ),
        ai_candidate_scorer_attach_to_order_plan=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_ATTACH_TO_ORDER_PLAN", "false")
        ),
        ai_candidate_scorer_attach_to_live_sim_run=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_ATTACH_TO_LIVE_SIM_RUN", "false")
        ),
        ai_candidate_scorer_max_prompt_chars=_parse_int(
            env.get("AI_CANDIDATE_SCORER_MAX_PROMPT_CHARS", "12000"),
            "AI_CANDIDATE_SCORER_MAX_PROMPT_CHARS",
            min_value=1000,
        ),
        ai_candidate_scorer_redact_account_id=_parse_bool(
            env.get("AI_CANDIDATE_SCORER_REDACT_ACCOUNT_ID", "true")
        ),
        ai_candidate_risk_reward_stop_loss_min=_parse_float(
            env.get("AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MIN", "1.0"),
            "AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MIN",
            min_value=0.0,
        ),
        ai_candidate_risk_reward_stop_loss_max=_parse_float(
            env.get("AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MAX", "4.0"),
            "AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MAX",
            min_value=0.0,
        ),
        ai_candidate_risk_reward_take_profit_min=_parse_float(
            env.get("AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MIN", "2.0"),
            "AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MIN",
            min_value=0.0,
        ),
        ai_candidate_risk_reward_take_profit_max=_parse_float(
            env.get("AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MAX", "8.0"),
            "AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MAX",
            min_value=0.0,
        ),
        ai_candidate_risk_reward_trailing_min=_parse_float(
            env.get("AI_CANDIDATE_RISK_REWARD_TRAILING_MIN", "1.0"),
            "AI_CANDIDATE_RISK_REWARD_TRAILING_MIN",
            min_value=0.0,
        ),
        ai_candidate_risk_reward_trailing_max=_parse_float(
            env.get("AI_CANDIDATE_RISK_REWARD_TRAILING_MAX", "4.0"),
            "AI_CANDIDATE_RISK_REWARD_TRAILING_MAX",
            min_value=0.0,
        ),
        ai_candidate_risk_reward_max_hold_min_sec=_parse_int(
            env.get("AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MIN_SEC", "300"),
            "AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MIN_SEC",
            min_value=1,
        ),
        ai_candidate_risk_reward_max_hold_max_sec=_parse_int(
            env.get("AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MAX_SEC", "7200"),
            "AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MAX_SEC",
            min_value=1,
        ),
        ai_external_llm_enabled=_parse_bool(env.get("AI_EXTERNAL_LLM_ENABLED", "false")),
        ai_external_llm_provider=env.get("AI_EXTERNAL_LLM_PROVIDER", "none"),
        ai_external_llm_model=env.get("AI_EXTERNAL_LLM_MODEL", ""),
        ai_external_llm_api_key_env=env.get(
            "AI_EXTERNAL_LLM_API_KEY_ENV",
            "OPENAI_API_KEY",
        ),
        ai_external_llm_base_url=env.get("AI_EXTERNAL_LLM_BASE_URL", ""),
        ai_external_llm_timeout_seconds=_parse_int(
            env.get("AI_EXTERNAL_LLM_TIMEOUT_SECONDS", "10"),
            "AI_EXTERNAL_LLM_TIMEOUT_SECONDS",
            min_value=1,
        ),
        ai_external_llm_max_retries=_parse_int(
            env.get("AI_EXTERNAL_LLM_MAX_RETRIES", "1"),
            "AI_EXTERNAL_LLM_MAX_RETRIES",
            min_value=0,
        ),
        ai_external_llm_retry_backoff_seconds=_parse_float(
            env.get("AI_EXTERNAL_LLM_RETRY_BACKOFF_SECONDS", "0.5"),
            "AI_EXTERNAL_LLM_RETRY_BACKOFF_SECONDS",
            min_value=0.0,
        ),
        ai_external_llm_max_response_chars=_parse_int(
            env.get("AI_EXTERNAL_LLM_MAX_RESPONSE_CHARS", "8000"),
            "AI_EXTERNAL_LLM_MAX_RESPONSE_CHARS",
            min_value=1,
        ),
        ai_external_llm_temperature=_parse_float(
            env.get("AI_EXTERNAL_LLM_TEMPERATURE", "0"),
            "AI_EXTERNAL_LLM_TEMPERATURE",
            min_value=0.0,
        ),
        ai_external_llm_require_json_schema=_parse_bool(
            env.get("AI_EXTERNAL_LLM_REQUIRE_JSON_SCHEMA", "true")
        ),
        ai_external_llm_store_request=_parse_bool(
            env.get("AI_EXTERNAL_LLM_STORE_REQUEST", "false")
        ),
        ai_external_llm_store_response=_parse_bool(
            env.get("AI_EXTERNAL_LLM_STORE_RESPONSE", "false")
        ),
        ai_external_llm_redact_prompt=_parse_bool(
            env.get("AI_EXTERNAL_LLM_REDACT_PROMPT", "true")
        ),
        ai_external_llm_fail_open=_parse_bool(env.get("AI_EXTERNAL_LLM_FAIL_OPEN", "true")),
        ai_external_llm_daily_call_limit=_parse_int(
            env.get("AI_EXTERNAL_LLM_DAILY_CALL_LIMIT", "100"),
            "AI_EXTERNAL_LLM_DAILY_CALL_LIMIT",
            min_value=0,
        ),
        ai_external_llm_per_run_call_limit=_parse_int(
            env.get("AI_EXTERNAL_LLM_PER_RUN_CALL_LIMIT", "1"),
            "AI_EXTERNAL_LLM_PER_RUN_CALL_LIMIT",
            min_value=0,
        ),
        ai_external_llm_cost_guard_enabled=_parse_bool(
            env.get("AI_EXTERNAL_LLM_COST_GUARD_ENABLED", "true")
        ),
        ai_external_llm_allow_network=_parse_bool(
            env.get("AI_EXTERNAL_LLM_ALLOW_NETWORK", "false")
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
        naver_theme_import_enabled=_parse_bool(
            env.get("NAVER_THEME_IMPORT_ENABLED", "false")
        ),
        naver_theme_import_base_url=env.get(
            "NAVER_THEME_IMPORT_BASE_URL",
            "https://finance.naver.com/sise/theme.naver",
        ),
        naver_theme_import_timeout_seconds=_parse_float(
            env.get("NAVER_THEME_IMPORT_TIMEOUT_SECONDS", "10"),
            "NAVER_THEME_IMPORT_TIMEOUT_SECONDS",
            min_value=0.0,
        ),
        naver_theme_import_max_themes=_parse_int(
            env.get("NAVER_THEME_IMPORT_MAX_THEMES", "50"),
            "NAVER_THEME_IMPORT_MAX_THEMES",
            min_value=1,
        ),
        naver_theme_import_request_sleep_seconds=_parse_float(
            env.get("NAVER_THEME_IMPORT_REQUEST_SLEEP_SECONDS", "0.3"),
            "NAVER_THEME_IMPORT_REQUEST_SLEEP_SECONDS",
            min_value=0.0,
        ),
        naver_theme_import_replace=_parse_bool(
            env.get("NAVER_THEME_IMPORT_REPLACE", "false")
        ),
        naver_theme_import_min_member_count=_parse_int(
            env.get("NAVER_THEME_IMPORT_MIN_MEMBER_COUNT", "2"),
            "NAVER_THEME_IMPORT_MIN_MEMBER_COUNT",
            min_value=1,
        ),
        naver_theme_import_abort_on_empty=_parse_bool(
            env.get("NAVER_THEME_IMPORT_ABORT_ON_EMPTY", "true")
        ),
        theme_leadership_enabled=_parse_bool(env.get("THEME_LEADERSHIP_ENABLED", "true")),
        theme_leadership_top_theme_count=_parse_int(
            env.get("THEME_LEADERSHIP_TOP_THEME_COUNT", "5"),
            "THEME_LEADERSHIP_TOP_THEME_COUNT",
            min_value=1,
        ),
        theme_leadership_max_stocks_per_theme=_parse_int(
            env.get("THEME_LEADERSHIP_MAX_STOCKS_PER_THEME", "3"),
            "THEME_LEADERSHIP_MAX_STOCKS_PER_THEME",
            min_value=1,
        ),
        theme_leadership_max_total_watchset=_parse_int(
            env.get("THEME_LEADERSHIP_MAX_TOTAL_WATCHSET", "20"),
            "THEME_LEADERSHIP_MAX_TOTAL_WATCHSET",
            min_value=1,
        ),
        theme_leadership_min_valid_members=_parse_int(
            env.get("THEME_LEADERSHIP_MIN_VALID_MEMBERS", "2"),
            "THEME_LEADERSHIP_MIN_VALID_MEMBERS",
            min_value=1,
        ),
        theme_leadership_min_fresh_coverage_ratio=_parse_float(
            env.get("THEME_LEADERSHIP_MIN_FRESH_COVERAGE_RATIO", "0.4"),
            "THEME_LEADERSHIP_MIN_FRESH_COVERAGE_RATIO",
        ),
        theme_leadership_condition_boost_enabled=_parse_bool(
            env.get("THEME_LEADERSHIP_CONDITION_BOOST_ENABLED", "true")
        ),
        theme_leadership_write_candidate_sources=_parse_bool(
            env.get("THEME_LEADERSHIP_WRITE_CANDIDATE_SOURCES", "false")
        ),
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
        strategy_engine_observe_only=_parse_bool(env.get("STRATEGY_ENGINE_OBSERVE_ONLY", "true")),
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
        strategy_engine_require_vwap=_parse_bool(env.get("STRATEGY_ENGINE_REQUIRE_VWAP", "false")),
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
        entry_timing_enabled=_parse_bool(env.get("ENTRY_TIMING_ENABLED", "true")),
        entry_timing_write_order_plan_drafts=_parse_bool(
            env.get("ENTRY_TIMING_WRITE_ORDER_PLAN_DRAFTS", "true")
        ),
        entry_timing_max_plans_per_run=_parse_int(
            env.get("ENTRY_TIMING_MAX_PLANS_PER_RUN", "20"),
            "ENTRY_TIMING_MAX_PLANS_PER_RUN",
            min_value=1,
        ),
        entry_timing_plan_ttl_seconds=_parse_int(
            env.get("ENTRY_TIMING_PLAN_TTL_SECONDS", "90"),
            "ENTRY_TIMING_PLAN_TTL_SECONDS",
            min_value=1,
        ),
        entry_timing_pullback_min_pct=_parse_float(
            env.get("ENTRY_TIMING_PULLBACK_MIN_PCT", "1.0"),
            "ENTRY_TIMING_PULLBACK_MIN_PCT",
            min_value=0.0,
        ),
        entry_timing_pullback_max_pct=_parse_float(
            env.get("ENTRY_TIMING_PULLBACK_MAX_PCT", "4.5"),
            "ENTRY_TIMING_PULLBACK_MAX_PCT",
            min_value=0.0,
        ),
        entry_timing_vwap_reclaim_tolerance_pct=_parse_float(
            env.get("ENTRY_TIMING_VWAP_RECLAIM_TOLERANCE_PCT", "0.7"),
            "ENTRY_TIMING_VWAP_RECLAIM_TOLERANCE_PCT",
            min_value=0.0,
        ),
        entry_timing_vwap_overextended_pct=_parse_float(
            env.get("ENTRY_TIMING_VWAP_OVEREXTENDED_PCT", "3.0"),
            "ENTRY_TIMING_VWAP_OVEREXTENDED_PCT",
            min_value=0.0,
        ),
        entry_timing_chase_near_high_pct=_parse_float(
            env.get("ENTRY_TIMING_CHASE_NEAR_HIGH_PCT", "0.7"),
            "ENTRY_TIMING_CHASE_NEAR_HIGH_PCT",
            min_value=0.0,
        ),
        entry_timing_max_spread_ticks=_parse_int(
            env.get("ENTRY_TIMING_MAX_SPREAD_TICKS", "3"),
            "ENTRY_TIMING_MAX_SPREAD_TICKS",
            min_value=1,
        ),
        entry_timing_min_turnover_krw=_parse_float(
            env.get("ENTRY_TIMING_MIN_TURNOVER_KRW", "500000000"),
            "ENTRY_TIMING_MIN_TURNOVER_KRW",
            min_value=0.0,
        ),
        entry_timing_min_execution_strength=_parse_float(
            env.get("ENTRY_TIMING_MIN_EXECUTION_STRENGTH", "100"),
            "ENTRY_TIMING_MIN_EXECUTION_STRENGTH",
            min_value=0.0,
        ),
        entry_timing_default_notional=_parse_float(
            env.get("ENTRY_TIMING_DEFAULT_NOTIONAL", "100000"),
            "ENTRY_TIMING_DEFAULT_NOTIONAL",
            min_value=0.0,
        ),
        entry_timing_max_notional=_parse_float(
            env.get("ENTRY_TIMING_MAX_NOTIONAL", "100000"),
            "ENTRY_TIMING_MAX_NOTIONAL",
            min_value=0.0,
        ),
        entry_timing_allow_market_order=_parse_bool(
            env.get("ENTRY_TIMING_ALLOW_MARKET_ORDER", "false")
        ),
        entry_timing_price_offset_ticks=_parse_int(
            env.get("ENTRY_TIMING_PRICE_OFFSET_TICKS", "0"),
            "ENTRY_TIMING_PRICE_OFFSET_TICKS",
            min_value=0,
        ),
        entry_timing_allow_follower_in_spreading=_parse_bool(
            env.get("ENTRY_TIMING_ALLOW_FOLLOWER_IN_SPREADING", "true")
        ),
        entry_timing_allow_follower_in_leader_only=_parse_bool(
            env.get("ENTRY_TIMING_ALLOW_FOLLOWER_IN_LEADER_ONLY", "false")
        ),
        entry_timing_require_risk_observe_pass=_parse_bool(
            env.get("ENTRY_TIMING_REQUIRE_RISK_OBSERVE_PASS", "false")
        ),
        entry_timing_require_strategy_matched=_parse_bool(
            env.get("ENTRY_TIMING_REQUIRE_STRATEGY_MATCHED", "false")
        ),
        entry_timing_stale_max_seconds=_parse_int(
            env.get("ENTRY_TIMING_STALE_MAX_SECONDS", "60"),
            "ENTRY_TIMING_STALE_MAX_SECONDS",
            min_value=1,
        ),
        entry_timing_config_version=env.get(
            "ENTRY_TIMING_CONFIG_VERSION",
            "entry_timing_v1",
        ),
        dry_run_oms_enabled=_parse_bool(env.get("DRY_RUN_OMS_ENABLED", "false")),
        dry_run_intent_creation_enabled=_parse_bool(
            env.get("DRY_RUN_INTENT_CREATION_ENABLED", "false")
        ),
        dry_run_simulated_fill_enabled=_parse_bool(
            env.get("DRY_RUN_SIMULATED_FILL_ENABLED", "false")
        ),
        dry_run_require_safety_gate=_parse_bool(env.get("DRY_RUN_REQUIRE_SAFETY_GATE", "true")),
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
        dry_run_exit_engine_enabled=_parse_bool(env.get("DRY_RUN_EXIT_ENGINE_ENABLED", "false")),
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
        live_sim_require_fresh_tick=_parse_bool(env.get("LIVE_SIM_REQUIRE_FRESH_TICK", "true")),
        live_sim_stale_tick_sec=_parse_int(
            env.get("LIVE_SIM_STALE_TICK_SEC", "15"),
            "LIVE_SIM_STALE_TICK_SEC",
            min_value=1,
        ),
        live_sim_allow_buy=_parse_bool(env.get("LIVE_SIM_ALLOW_BUY", "true")),
        live_sim_allow_sell=_parse_bool(env.get("LIVE_SIM_ALLOW_SELL", "false")),
        live_sim_allow_exit_sell=_parse_bool(env.get("LIVE_SIM_ALLOW_EXIT_SELL", "false")),
        live_sim_allow_market_order=_parse_bool(env.get("LIVE_SIM_ALLOW_MARKET_ORDER", "false")),
        live_sim_allow_limit_order=_parse_bool(env.get("LIVE_SIM_ALLOW_LIMIT_ORDER", "true")),
        live_sim_default_order_type=env.get("LIVE_SIM_DEFAULT_ORDER_TYPE", "LIMIT"),
        live_sim_default_hoga=env.get("LIVE_SIM_DEFAULT_HOGA", "00"),
        live_sim_price_offset_ticks=_parse_int(
            env.get("LIVE_SIM_PRICE_OFFSET_TICKS", "0"),
            "LIVE_SIM_PRICE_OFFSET_TICKS",
            min_value=0,
        ),
        live_sim_config_version=env.get("LIVE_SIM_CONFIG_VERSION", "live_sim_v1"),
        live_sim_pilot_pipeline_enabled=_parse_bool(
            env.get("LIVE_SIM_PILOT_PIPELINE_ENABLED", "false")
        ),
        live_sim_pilot_auto_queue_command=_parse_bool(
            env.get("LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND", "false")
        ),
        live_sim_order_plan_routing_enabled=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED", "false")
        ),
        live_sim_order_plan_require_plan_ready=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_REQUIRE_PLAN_READY", "true")
        ),
        live_sim_order_plan_require_fresh_tick=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_REQUIRE_FRESH_TICK", "true")
        ),
        live_sim_order_plan_stale_sec=_parse_int(
            env.get("LIVE_SIM_ORDER_PLAN_STALE_SEC", "30"),
            "LIVE_SIM_ORDER_PLAN_STALE_SEC",
            min_value=1,
        ),
        live_sim_order_plan_max_price_drift_pct=_parse_float(
            env.get("LIVE_SIM_ORDER_PLAN_MAX_PRICE_DRIFT_PCT", "0.8"),
            "LIVE_SIM_ORDER_PLAN_MAX_PRICE_DRIFT_PCT",
            min_value=0.0,
        ),
        live_sim_order_plan_require_strategy_matched=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_REQUIRE_STRATEGY_MATCHED", "true")
        ),
        live_sim_order_plan_require_risk_observe_pass=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_REQUIRE_RISK_OBSERVE_PASS", "true")
        ),
        live_sim_order_plan_require_candidate_context_ready=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_REQUIRE_CANDIDATE_CONTEXT_READY", "true")
        ),
        live_sim_order_plan_require_dry_run_evidence=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_REQUIRE_DRY_RUN_EVIDENCE", "false")
        ),
        live_sim_order_plan_max_plans_per_run=_parse_int(
            env.get("LIVE_SIM_ORDER_PLAN_MAX_PLANS_PER_RUN", "3"),
            "LIVE_SIM_ORDER_PLAN_MAX_PLANS_PER_RUN",
            min_value=1,
        ),
        live_sim_order_plan_max_commands_per_run=_parse_int(
            env.get("LIVE_SIM_ORDER_PLAN_MAX_COMMANDS_PER_RUN", "1"),
            "LIVE_SIM_ORDER_PLAN_MAX_COMMANDS_PER_RUN",
            min_value=1,
        ),
        live_sim_order_plan_min_notional=_parse_float(
            env.get("LIVE_SIM_ORDER_PLAN_MIN_NOTIONAL", "10000"),
            "LIVE_SIM_ORDER_PLAN_MIN_NOTIONAL",
            min_value=0.0,
        ),
        live_sim_order_plan_default_notional=_parse_float(
            env.get("LIVE_SIM_ORDER_PLAN_DEFAULT_NOTIONAL", "100000"),
            "LIVE_SIM_ORDER_PLAN_DEFAULT_NOTIONAL",
            min_value=0.0,
        ),
        live_sim_order_plan_max_notional=_parse_float(
            env.get("LIVE_SIM_ORDER_PLAN_MAX_NOTIONAL", "100000"),
            "LIVE_SIM_ORDER_PLAN_MAX_NOTIONAL",
            min_value=0.0,
        ),
        live_sim_order_plan_allow_market_order=_parse_bool(
            env.get("LIVE_SIM_ORDER_PLAN_ALLOW_MARKET_ORDER", "false")
        ),
        live_sim_order_plan_allowed_side=env.get(
            "LIVE_SIM_ORDER_PLAN_ALLOWED_SIDE",
            "BUY",
        ),
        live_sim_fee_rate=_parse_float(
            env.get("LIVE_SIM_FEE_RATE", "0.0"),
            "LIVE_SIM_FEE_RATE",
            min_value=0.0,
        ),
        live_sim_tax_rate=_parse_float(
            env.get("LIVE_SIM_TAX_RATE", "0.0"),
            "LIVE_SIM_TAX_RATE",
            min_value=0.0,
        ),
        live_sim_position_allow_scale_in=_parse_bool(
            env.get("LIVE_SIM_POSITION_ALLOW_SCALE_IN", "false")
        ),
        live_sim_position_max_per_code=_parse_int(
            env.get("LIVE_SIM_POSITION_MAX_PER_CODE", "1"),
            "LIVE_SIM_POSITION_MAX_PER_CODE",
            min_value=1,
        ),
        live_sim_cancel_enabled=_parse_bool(env.get("LIVE_SIM_CANCEL_ENABLED", "false")),
        live_sim_cancel_unfilled_enabled=_parse_bool(
            env.get("LIVE_SIM_CANCEL_UNFILLED_ENABLED", "false")
        ),
        live_sim_cancel_order_ttl_sec=_parse_int(
            env.get("LIVE_SIM_CANCEL_ORDER_TTL_SEC", "60"),
            "LIVE_SIM_CANCEL_ORDER_TTL_SEC",
            min_value=1,
        ),
        live_sim_cancel_max_commands_per_run=_parse_int(
            env.get("LIVE_SIM_CANCEL_MAX_COMMANDS_PER_RUN", "3"),
            "LIVE_SIM_CANCEL_MAX_COMMANDS_PER_RUN",
            min_value=1,
        ),
        live_sim_cancel_require_broker_order_no=_parse_bool(
            env.get("LIVE_SIM_CANCEL_REQUIRE_BROKER_ORDER_NO", "true")
        ),
        live_sim_cancel_allow_without_broker_order_no=_parse_bool(
            env.get("LIVE_SIM_CANCEL_ALLOW_WITHOUT_BROKER_ORDER_NO", "false")
        ),
        live_sim_cancel_kill_switch=_parse_bool(
            env.get("LIVE_SIM_CANCEL_KILL_SWITCH", "false")
        ),
        live_sim_exit_engine_enabled=_parse_bool(
            env.get("LIVE_SIM_EXIT_ENGINE_ENABLED", "false")
        ),
        live_sim_exit_order_creation_enabled=_parse_bool(
            env.get("LIVE_SIM_EXIT_ORDER_CREATION_ENABLED", "false")
        ),
        live_sim_exit_gateway_command_enabled=_parse_bool(
            env.get("LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED", "false")
        ),
        live_sim_exit_allow_sell_close_only=_parse_bool(
            env.get("LIVE_SIM_EXIT_ALLOW_SELL_CLOSE_ONLY", "true")
        ),
        live_sim_exit_allow_short=_parse_bool(env.get("LIVE_SIM_EXIT_ALLOW_SHORT", "false")),
        live_sim_exit_default_order_type=env.get("LIVE_SIM_EXIT_DEFAULT_ORDER_TYPE", "LIMIT"),
        live_sim_exit_allow_market_order=_parse_bool(
            env.get("LIVE_SIM_EXIT_ALLOW_MARKET_ORDER", "false")
        ),
        live_sim_exit_use_market_for_stop=_parse_bool(
            env.get("LIVE_SIM_EXIT_USE_MARKET_FOR_STOP", "false")
        ),
        live_sim_exit_stop_loss_pct=_parse_float(
            env.get("LIVE_SIM_EXIT_STOP_LOSS_PCT", "3.0"),
            "LIVE_SIM_EXIT_STOP_LOSS_PCT",
            min_value=0.0,
        ),
        live_sim_exit_take_profit_pct=_parse_float(
            env.get("LIVE_SIM_EXIT_TAKE_PROFIT_PCT", "5.0"),
            "LIVE_SIM_EXIT_TAKE_PROFIT_PCT",
            min_value=0.0,
        ),
        live_sim_exit_trailing_stop_pct=_parse_float(
            env.get("LIVE_SIM_EXIT_TRAILING_STOP_PCT", "2.5"),
            "LIVE_SIM_EXIT_TRAILING_STOP_PCT",
            min_value=0.0,
        ),
        live_sim_exit_trailing_activation_pct=_parse_float(
            env.get("LIVE_SIM_EXIT_TRAILING_ACTIVATION_PCT", "2.0"),
            "LIVE_SIM_EXIT_TRAILING_ACTIVATION_PCT",
            min_value=0.0,
        ),
        live_sim_exit_max_hold_sec=_parse_int(
            env.get("LIVE_SIM_EXIT_MAX_HOLD_SEC", "1800"),
            "LIVE_SIM_EXIT_MAX_HOLD_SEC",
            min_value=1,
        ),
        live_sim_exit_min_hold_sec=_parse_int(
            env.get("LIVE_SIM_EXIT_MIN_HOLD_SEC", "30"),
            "LIVE_SIM_EXIT_MIN_HOLD_SEC",
            min_value=0,
        ),
        live_sim_exit_eod_flatten_enabled=_parse_bool(
            env.get("LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED", "false")
        ),
        live_sim_exit_eod_flatten_time=env.get(
            "LIVE_SIM_EXIT_EOD_FLATTEN_TIME",
            "15:15:00",
        ),
        live_sim_exit_max_commands_per_run=_parse_int(
            env.get("LIVE_SIM_EXIT_MAX_COMMANDS_PER_RUN", "3"),
            "LIVE_SIM_EXIT_MAX_COMMANDS_PER_RUN",
            min_value=1,
        ),
        live_sim_exit_price_offset_ticks=_parse_int(
            env.get("LIVE_SIM_EXIT_PRICE_OFFSET_TICKS", "0"),
            "LIVE_SIM_EXIT_PRICE_OFFSET_TICKS",
            min_value=0,
        ),
        live_sim_reconcile_enabled=_parse_bool(
            env.get("LIVE_SIM_RECONCILE_ENABLED", "true")
        ),
        live_sim_reconcile_request_broker_snapshot_enabled=_parse_bool(
            env.get("LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED", "false")
        ),
        live_sim_reconcile_block_new_buy_on_mismatch=_parse_bool(
            env.get("LIVE_SIM_RECONCILE_BLOCK_NEW_BUY_ON_MISMATCH", "true")
        ),
        live_sim_reconcile_allow_exit_on_mismatch=_parse_bool(
            env.get("LIVE_SIM_RECONCILE_ALLOW_EXIT_ON_MISMATCH", "true")
        ),
        live_sim_reconcile_stale_order_sec=_parse_int(
            env.get("LIVE_SIM_RECONCILE_STALE_ORDER_SEC", "300"),
            "LIVE_SIM_RECONCILE_STALE_ORDER_SEC",
            min_value=1,
        ),
        live_sim_operating_cycle_enabled=_parse_bool(
            env.get("LIVE_SIM_OPERATING_CYCLE_ENABLED", "true")
        ),
        live_sim_operating_default_mode=env.get(
            "LIVE_SIM_OPERATING_DEFAULT_MODE",
            "OBSERVE_CYCLE",
        ),
        live_sim_operating_max_buy_commands_per_cycle=_parse_int(
            env.get("LIVE_SIM_OPERATING_MAX_BUY_COMMANDS_PER_CYCLE", "1"),
            "LIVE_SIM_OPERATING_MAX_BUY_COMMANDS_PER_CYCLE",
            min_value=0,
        ),
        live_sim_operating_max_cancel_commands_per_cycle=_parse_int(
            env.get("LIVE_SIM_OPERATING_MAX_CANCEL_COMMANDS_PER_CYCLE", "3"),
            "LIVE_SIM_OPERATING_MAX_CANCEL_COMMANDS_PER_CYCLE",
            min_value=0,
        ),
        live_sim_operating_max_exit_commands_per_cycle=_parse_int(
            env.get("LIVE_SIM_OPERATING_MAX_EXIT_COMMANDS_PER_CYCLE", "3"),
            "LIVE_SIM_OPERATING_MAX_EXIT_COMMANDS_PER_CYCLE",
            min_value=0,
        ),
        live_sim_operating_require_preflight_pass_for_queue=_parse_bool(
            env.get("LIVE_SIM_OPERATING_REQUIRE_PREFLIGHT_PASS_FOR_QUEUE", "true")
        ),
        live_sim_operating_include_ai=_parse_bool(
            env.get("LIVE_SIM_OPERATING_INCLUDE_AI", "true")
        ),
        live_sim_operating_include_no_buy=_parse_bool(
            env.get("LIVE_SIM_OPERATING_INCLUDE_NO_BUY", "true")
        ),
        live_sim_operating_write_runs=_parse_bool(
            env.get("LIVE_SIM_OPERATING_WRITE_RUNS", "true")
        ),
        no_buy_sentinel_enabled=_parse_bool(env.get("NO_BUY_SENTINEL_ENABLED", "true")),
        no_buy_sentinel_market_open_time=env.get(
            "NO_BUY_SENTINEL_MARKET_OPEN_TIME",
            "09:00:00",
        ),
        no_buy_sentinel_minutes_after_open=_parse_int(
            env.get("NO_BUY_SENTINEL_MINUTES_AFTER_OPEN", "20"),
            "NO_BUY_SENTINEL_MINUTES_AFTER_OPEN",
            min_value=1,
        ),
        no_buy_sentinel_top_near_miss_limit=_parse_int(
            env.get("NO_BUY_SENTINEL_TOP_NEAR_MISS_LIMIT", "10"),
            "NO_BUY_SENTINEL_TOP_NEAR_MISS_LIMIT",
            min_value=1,
        ),
        no_buy_sentinel_lookback_minutes=_parse_int(
            env.get("NO_BUY_SENTINEL_LOOKBACK_MINUTES", "60"),
            "NO_BUY_SENTINEL_LOOKBACK_MINUTES",
            min_value=1,
        ),
        no_buy_sentinel_include_ai=_parse_bool(
            env.get("NO_BUY_SENTINEL_INCLUDE_AI", "true")
        ),
        no_buy_sentinel_include_config=_parse_bool(
            env.get("NO_BUY_SENTINEL_INCLUDE_CONFIG", "true")
        ),
        no_buy_sentinel_include_reconcile=_parse_bool(
            env.get("NO_BUY_SENTINEL_INCLUDE_RECONCILE", "true")
        ),
        no_buy_sentinel_write_snapshots=_parse_bool(
            env.get("NO_BUY_SENTINEL_WRITE_SNAPSHOTS", "true")
        ),
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


def _parse_trading_profile(value: str) -> TradingProfile:
    normalized = value.strip().upper()
    try:
        return TradingProfile(normalized)
    except ValueError as exc:
        allowed = ", ".join(profile.value for profile in TradingProfile)
        raise ValueError(
            f"Unsupported TRADING_PROFILE={value!r}; expected one of: {allowed}"
        ) from exc


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


def _validate_time_string(value: str, field_name: str) -> str:
    normalized = _require_non_empty_config(value)
    parts = normalized.split(":")
    if len(parts) != 3:
        raise ValueError(f"{field_name} must be HH:MM:SS")
    try:
        hour, minute, second = (int(part) for part in parts)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be HH:MM:SS") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59 or second < 0 or second > 59:
        raise ValueError(f"{field_name} must be HH:MM:SS")
    return f"{hour:02d}:{minute:02d}:{second:02d}"


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
