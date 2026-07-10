from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta, timezone, tzinfo
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from domain.market.bars import normalize_interval_list

DEFAULT_DB_PATH = "storage/suseok-trader-v2.sqlite3"
DEFAULT_ENV_FILE_PATH = Path(__file__).resolve().parents[1] / ".env"
ENV_FILE_PATH_ENV = "TRADING_ENV_FILE"


class TradingMode(StrEnum):
    OBSERVE = "OBSERVE"
    LIVE_SIM = "LIVE_SIM"
    LIVE_REAL = "LIVE_REAL"


class TradingProfile(StrEnum):
    OBSERVE = "OBSERVE"
    LIVE_SIM_PILOT = "LIVE_SIM_PILOT"


@dataclass(frozen=True)
class TradingCapabilities:
    profile: TradingProfile
    observation_allowed: bool
    dry_run_shadow_allowed: bool
    live_sim_intent_allowed: bool
    live_sim_order_plan_allowed: bool
    live_sim_gateway_command_allowed: bool
    live_real_order_allowed: bool
    broker_order_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile.value,
            "observation_allowed": self.observation_allowed,
            "dry_run_shadow_allowed": self.dry_run_shadow_allowed,
            "live_sim_intent_allowed": self.live_sim_intent_allowed,
            "live_sim_order_plan_allowed": self.live_sim_order_plan_allowed,
            "live_sim_gateway_command_allowed": self.live_sim_gateway_command_allowed,
            "live_real_order_allowed": self.live_real_order_allowed,
            "broker_order_path": self.broker_order_path,
        }


@dataclass(frozen=True)
class DeprecatedFlagWarning:
    flag: str
    status: str
    replacement: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "flag": self.flag,
            "status": self.status,
            "replacement": self.replacement,
            "message": self.message,
        }


_TRADING_CAPABILITY_MATRIX = {
    TradingProfile.OBSERVE: TradingCapabilities(
        profile=TradingProfile.OBSERVE,
        observation_allowed=True,
        dry_run_shadow_allowed=False,
        live_sim_intent_allowed=False,
        live_sim_order_plan_allowed=False,
        live_sim_gateway_command_allowed=False,
        live_real_order_allowed=False,
        broker_order_path="OBSERVE_ONLY",
    ),
    TradingProfile.LIVE_SIM_PILOT: TradingCapabilities(
        profile=TradingProfile.LIVE_SIM_PILOT,
        observation_allowed=True,
        dry_run_shadow_allowed=True,
        live_sim_intent_allowed=True,
        live_sim_order_plan_allowed=True,
        live_sim_gateway_command_allowed=True,
        live_real_order_allowed=False,
        broker_order_path="LIVE_SIM_ONLY",
    ),
}


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
    market_data_premarket_snapshot_enabled: bool = False
    market_data_projection_reconcile_limit: int = 500
    market_data_reconcile_live_default_persist: bool = False
    market_data_reconcile_locked_fallback_to_read_only: bool = True
    operator_sqlite_lock_retry_attempts: int = 3
    operator_sqlite_lock_retry_base_sleep_sec: float = 0.05
    operator_sqlite_lock_retry_max_sleep_sec: float = 0.5
    operator_sqlite_busy_timeout_ms: int = 500
    operator_run_once_locked_http_status: int = 409
    ops_script_locked_retry_attempts: int = 3
    ops_script_locked_retry_sleep_sec: float = 1.0
    gateway_market_data_append_only_dry_run_enabled: bool = False
    gateway_market_data_append_only_cutover_enabled: bool = False
    gateway_market_data_append_only_operating_mode: str = "OFF"
    gateway_market_data_append_only_global_kill_switch: bool = True
    gateway_market_data_append_only_auto_rollback_enabled: bool = True
    gateway_market_data_append_only_global_max_skip_per_minute: int = 0
    gateway_market_data_append_only_max_error_count: int = 0
    gateway_market_data_append_only_max_dead_letter_count: int = 0
    gateway_market_data_append_only_max_pending_within_sla: int = 100
    gateway_market_data_append_only_max_condition_event_pending_within_sla: int = 10
    gateway_market_data_append_only_require_dashboard_fast_ok: bool = True
    gateway_market_data_append_only_require_backlog_ready: bool = True
    gateway_market_data_append_only_auto_rollback_cooldown_sec: int = 300
    gateway_market_data_append_only_health_stale_sec: int = 60
    gateway_market_data_append_only_price_tick_cutover_enabled: bool = False
    gateway_market_data_append_only_tr_response_dry_run_enabled: bool = False
    gateway_market_data_append_only_tr_response_cutover_enabled: bool = False
    gateway_market_data_append_only_tr_response_require_worker_side_effects: bool = True
    gateway_market_data_append_only_tr_response_max_skip_per_minute: int = 0
    gateway_market_data_append_only_tr_response_require_synthetic_child_guard: bool = True
    gateway_market_data_append_only_tr_response_max_rows_per_event: int = 50
    gateway_market_data_append_only_tr_response_fail_closed_on_side_effect_error: bool = True
    gateway_market_data_append_only_condition_event_dry_run_enabled: bool = False
    gateway_market_data_append_only_condition_event_cutover_enabled: bool = False
    gateway_market_data_append_only_condition_event_require_worker_side_effects: bool = True
    gateway_market_data_append_only_condition_event_require_fusion_enabled: bool = True
    gateway_market_data_append_only_condition_event_require_backlog_ready: bool = True
    gateway_market_data_append_only_condition_event_max_skip_per_minute: int = 0
    gateway_market_data_append_only_condition_event_fail_closed_on_side_effect_error: bool = True
    gateway_market_data_append_only_condition_event_allow_candidate_ingest_in_worker: bool = False
    gateway_market_data_append_only_condition_event_max_payload_age_sec: int = 60
    gateway_market_data_append_only_cutover_event_types: tuple[str, ...] = ("price_tick",)
    gateway_market_data_append_only_require_reconcile_pass: bool = True
    gateway_market_data_append_only_require_latest_reconcile_pass: bool = True
    gateway_market_data_append_only_require_worker_apply_enabled: bool = True
    gateway_market_data_append_only_fail_closed_on_routing_error: bool = True
    gateway_market_data_append_only_price_tick_max_skip_per_minute: int = 0
    gateway_market_data_append_only_reconcile_max_age_sec: int = 300
    gateway_market_data_append_only_event_types: tuple[str, ...] = (
        "price_tick",
        "condition_event",
        "tr_response",
    )
    gateway_market_data_append_only_min_outbox_status: str = "ENQUEUED"
    gateway_market_reference_append_only_dry_run_enabled: bool = False
    gateway_market_reference_append_only_cutover_enabled: bool = False
    gateway_market_reference_append_only_global_kill_switch: bool = True
    gateway_market_reference_append_only_max_skip_per_minute: int = 0
    gateway_market_reference_append_only_max_pending_within_sla: int = 1
    gateway_market_reference_append_only_require_reconcile_pass: bool = True
    gateway_market_reference_append_only_reconcile_max_age_sec: int = 300
    gateway_market_reference_append_only_min_membership_count: int = 100
    gateway_market_reference_append_only_effective_skip_disabled_in_pr13: bool = True
    gateway_market_index_append_only_dry_run_enabled: bool = False
    gateway_market_index_append_only_cutover_enabled: bool = False
    gateway_market_index_append_only_global_kill_switch: bool = True
    gateway_market_index_append_only_max_skip_per_minute: int = 0
    gateway_market_index_append_only_max_pending_within_sla: int = 1
    gateway_market_index_append_only_require_reconcile_pass: bool = True
    gateway_market_index_append_only_require_data_usable: bool = True
    gateway_market_index_append_only_require_parser_verified: bool = True
    gateway_market_index_append_only_require_worker_regime_refresh: bool = True
    gateway_market_index_append_only_fail_closed_on_regime_refresh_error: bool = True
    gateway_market_index_append_only_reconcile_max_age_sec: int = 300
    gateway_market_index_append_only_max_event_age_sec: int = 30
    gateway_market_index_append_only_max_future_skew_sec: int = 5
    gateway_market_index_append_only_require_fresh_gateway_health: bool = True
    gateway_market_index_append_only_gateway_health_max_age_sec: int = 30
    gateway_market_index_append_only_effective_skip_disabled_in_pr15: bool = True
    gateway_market_regime_append_only_dry_run_enabled: bool = False
    gateway_market_regime_append_only_cutover_enabled: bool = False
    gateway_market_regime_append_only_global_kill_switch: bool = True
    gateway_market_regime_append_only_max_skip_per_minute: int = 0
    gateway_market_regime_append_only_max_pending_within_sla: int = 1
    gateway_market_regime_append_only_require_reconcile_pass: bool = True
    gateway_market_regime_append_only_require_prior_event_reconcile: bool = True
    gateway_market_regime_append_only_require_index_routing_guard: bool = True
    gateway_market_regime_append_only_require_worker_context_refresh: bool = True
    gateway_market_regime_append_only_fail_closed_on_context_refresh_error: bool = True
    gateway_market_regime_append_only_reconcile_max_age_sec: int = 300
    gateway_market_regime_append_only_effective_skip_disabled_in_pr18: bool = True
    projection_event_result_backfill_enabled: bool = False
    event_store_retention_enabled: bool = False
    event_store_retention_days: int = 30
    event_store_retention_batch_size: int = 5000
    event_store_retention_interval_sec: int = 86400
    market_regime_enabled: bool = True
    market_context_snapshot_stale_sec: int = 30
    market_index_stale_sec: int = 30
    market_scan_enabled: bool = False
    market_scan_interval_sec: int = 120
    market_scan_top_n: int = 200
    market_scan_tr_codes: Mapping[str, str] = field(
        default_factory=lambda: {
            "TRADE_VALUE": "OPT10032",
            "CHANGE_RATE": "OPT10027",
        }
    )
    market_scan_markets: tuple[str, ...] = ("KOSPI", "KOSDAQ")
    market_scan_market_codes: Mapping[str, str] = field(
        default_factory=lambda: {
            "KOSPI": "001",
            "KOSDAQ": "101",
        }
    )
    market_scan_screen_no: str = "8800"
    market_scan_parser_status: str = "PILOT_UNVERIFIED"
    market_regime_risk_on_return_5m: float = 0.15
    market_regime_weak_drawdown_15m: float = -0.40
    market_regime_risk_off_return_5m: float = -0.35
    market_regime_risk_off_drawdown_15m: float = -0.80
    market_regime_secondary_risk_off_return_5m: float = -0.60
    realtime_subscription_enabled: bool = True
    realtime_subscription_queue_commands: bool = False
    realtime_subscription_max_total: int = 50
    realtime_subscription_max_per_theme: int = 5
    realtime_subscription_anchor_codes: tuple[str, ...] = ("005930", "000660")
    realtime_subscription_stale_sec: int = 60
    realtime_subscription_remove_stale_after_sec: int = 600
    realtime_subscription_allow_remove: bool = False
    realtime_subscription_exchange: str = "KRX"
    theme_service_enabled: bool = True
    theme_min_active_members: int = 2
    theme_min_fresh_coverage_ratio: float = 0.3
    theme_observable_coverage_enabled: bool = True
    theme_min_observable_members: int = 3
    theme_leading_rising_ratio: float = 0.5
    theme_spreading_rising_ratio: float = 0.35
    theme_min_total_trade_value: float = 0.0
    theme_leader_min_change_rate: float = 0.0
    theme_leader_min_trade_value_delta_1m: float = 0.0
    theme_co_leader_score_ratio: float = 0.8
    theme_snapshot_max_members: int = 200
    theme_snapshot_stale_sec: int = 300
    theme_premarket_observables_enabled: bool = False
    theme_import_allow_replace: bool = False
    naver_theme_import_enabled: bool = False
    naver_theme_import_base_url: str = "https://finance.naver.com/sise/theme.naver"
    naver_theme_import_timeout_seconds: float = 10.0
    naver_theme_import_max_themes: int = 500
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
    condition_fusion_event_incremental_enabled: bool = True
    condition_fusion_sweep_enabled: bool = True
    condition_fusion_sweep_interval_sec: int = 60
    incremental_evaluation_enabled: bool = True
    incremental_evaluation_worker_enabled: bool = True
    incremental_evaluation_worker_interval_sec: float = 1.0
    incremental_evaluation_batch_size: int = 20
    incremental_evaluation_retry_limit: int = 3
    projection_outbox_worker_enabled: bool = False
    projection_outbox_worker_interval_sec: float = 1.0
    projection_outbox_batch_size: int = 100
    projection_outbox_retry_limit: int = 3
    projection_outbox_processing_ttl_sec: int = 60
    projection_outbox_shadow_mode: bool = True
    projection_outbox_apply_projection_enabled: bool = False
    projection_outbox_market_data_apply_enabled: bool = False
    projection_outbox_market_reference_apply_enabled: bool = False
    projection_outbox_market_index_apply_enabled: bool = False
    projection_outbox_market_regime_apply_enabled: bool = False
    projection_outbox_apply_batch_size: int = 50
    projection_outbox_market_reference_apply_batch_size: int = 20
    projection_outbox_market_index_apply_batch_size: int = 20
    projection_outbox_market_regime_apply_batch_size: int = 20
    projection_outbox_live_run_once_batch_size: int = 50
    projection_outbox_run_once_max_wall_ms: int = 5000
    projection_outbox_apply_min_age_sec: float = 1.0
    projection_outbox_market_reference_apply_min_age_sec: float = 1.0
    projection_outbox_market_index_apply_min_age_sec: float = 1.0
    projection_outbox_market_regime_apply_min_age_sec: float = 1.0
    projection_outbox_shadow_min_age_sec: float = 0.5
    projection_outbox_backlog_warn_pending_count: int = 1000
    projection_outbox_backlog_fail_pending_count: int = 10000
    projection_outbox_backlog_recent_window_sec: int = 300
    projection_outbox_backlog_recent_fail_count: int = 100
    projection_outbox_backlog_stale_processing_sec: int = 120
    projection_outbox_backlog_condition_event_ready_max_pending: int = 100
    projection_outbox_backlog_condition_event_ready_recent_max_pending: int = 10
    projection_outbox_backlog_required_for_condition_event_cutover: bool = True
    candidate_fsm_enabled: bool = True
    candidate_trade_date_timezone: str = "Asia/Seoul"
    candidate_source_stale_sec: int = 300
    candidate_tick_stale_sec: int = 90
    candidate_stale_requires_tick_stale: bool = True
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
    risk_cross_exchange_divergence_bp: float = 0.0
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
    entry_timing_premarket_context_enabled: bool = False
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
    live_sim_order_exchange: str = "KRX"
    live_sim_nxt_support_confirmed: bool = False
    live_sim_kill_switch: bool = True
    live_sim_max_order_notional: float = 100_000
    live_sim_max_daily_order_count: int = 3
    live_sim_max_daily_notional: float = 300_000
    live_sim_max_daily_loss: float = 0.0
    live_sim_max_daily_loss_pct: float = 0.0
    live_sim_max_active_orders: int = 1
    live_sim_max_active_positions: int = 1
    live_sim_duplicate_cooldown_sec: int = 600
    live_sim_order_ttl_sec: int = 60
    live_sim_preflight_pending_command_backlog_warn_threshold: int = 30
    live_sim_require_dry_run_evidence: bool = True
    live_sim_require_risk_observe_pass: bool = True
    live_sim_require_strategy_matched: bool = True
    live_sim_require_candidate_context_ready: bool = True
    live_sim_require_fresh_tick: bool = True
    live_sim_stale_tick_sec: int = 15
    live_sim_entry_window_start: str = "09:05:00"
    live_sim_entry_window_end: str = "14:30:00"
    live_sim_allow_buy: bool = True
    live_sim_allow_sell: bool = False
    live_sim_allow_exit_sell: bool = False
    live_sim_allow_market_order: bool = False
    live_sim_allow_limit_order: bool = True
    live_sim_default_order_type: str = "LIMIT"
    live_sim_default_hoga: str = "00"
    live_sim_price_offset_ticks: int = 0
    live_sim_buy_price_offset_ticks: int = 1
    live_sim_reprice_enabled: bool = False
    live_sim_reprice_max_attempts: int = 1
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
    live_sim_reconcile_notional_tolerance: float = 1.0
    live_sim_operating_cycle_enabled: bool = True
    live_sim_operating_default_mode: str = "OBSERVE_CYCLE"
    live_sim_operating_max_buy_commands_per_cycle: int = 1
    live_sim_operating_max_cancel_commands_per_cycle: int = 3
    live_sim_operating_max_exit_commands_per_cycle: int = 3
    live_sim_operating_require_preflight_pass_for_queue: bool = True
    live_sim_operating_include_ai: bool = True
    live_sim_operating_include_no_buy: bool = True
    live_sim_operating_write_runs: bool = True
    live_sim_operating_loop_enabled: bool = False
    live_sim_operating_loop_queue_commands: bool = False
    live_sim_operating_loop_interval_sec: int = 20
    live_sim_operating_loop_market_open_time: str = "09:05:00"
    live_sim_operating_loop_market_close_time: str = "15:20:00"
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
    dashboard_snapshot_sections_enabled: bool = True
    dashboard_snapshot_fast_cache_ttl_sec: float = 2.0
    dashboard_snapshot_fast_default_limit: int = 20
    dashboard_snapshot_fast_timeout_budget_ms: int = 5000
    dashboard_snapshot_warn_latency_ms: int = 3000
    dashboard_snapshot_fail_latency_ms: int = 10000
    deprecated_flag_warnings: tuple[DeprecatedFlagWarning, ...] = ()

    def __post_init__(self) -> None:
        if self.market_data_degraded_tick_stale_sec < self.market_data_tick_stale_sec:
            raise ValueError(
                "MARKET_DATA_DEGRADED_TICK_STALE_SEC must be >= MARKET_DATA_TICK_STALE_SEC"
            )
        if self.market_data_projection_reconcile_limit < 1:
            raise ValueError("MARKET_DATA_PROJECTION_RECONCILE_LIMIT must be >= 1")
        for field_name in (
            "operator_sqlite_lock_retry_attempts",
            "operator_sqlite_busy_timeout_ms",
            "operator_run_once_locked_http_status",
            "ops_script_locked_retry_attempts",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.operator_run_once_locked_http_status not in {200, 409}:
            raise ValueError("OPERATOR_RUN_ONCE_LOCKED_HTTP_STATUS must be 200 or 409")
        for field_name in (
            "operator_sqlite_lock_retry_base_sleep_sec",
            "operator_sqlite_lock_retry_max_sleep_sec",
            "ops_script_locked_retry_sleep_sec",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        if (
            self.operator_sqlite_lock_retry_max_sleep_sec
            < self.operator_sqlite_lock_retry_base_sleep_sec
        ):
            raise ValueError(
                "OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC must be >= "
                "OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC"
            )
        if self.gateway_market_data_append_only_reconcile_max_age_sec < 1:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC must be >= 1"
            )
        normalized_operating_mode = (
            self.gateway_market_data_append_only_operating_mode.strip().upper()
        )
        allowed_operating_modes = {
            "OFF",
            "DRY_RUN",
            "PRICE_TICK_ONLY",
            "TR_RESPONSE_ONLY",
            "CONDITION_EVENT_ONLY",
            "MARKET_DATA_LIMITED",
            "MARKET_DATA_FULL_GUARDED",
        }
        if normalized_operating_mode not in allowed_operating_modes:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE must be one of "
                f"{sorted(allowed_operating_modes)}"
            )
        object.__setattr__(
            self,
            "gateway_market_data_append_only_operating_mode",
            normalized_operating_mode,
        )
        for field_name in (
            "gateway_market_data_append_only_global_max_skip_per_minute",
            "gateway_market_data_append_only_max_error_count",
            "gateway_market_data_append_only_max_dead_letter_count",
            "gateway_market_data_append_only_max_pending_within_sla",
            "gateway_market_data_append_only_max_condition_event_pending_within_sla",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        for field_name in (
            "gateway_market_data_append_only_auto_rollback_cooldown_sec",
            "gateway_market_data_append_only_health_stale_sec",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        object.__setattr__(
            self,
            "gateway_market_data_append_only_event_types",
            tuple(
                event_type.strip().lower()
                for event_type in self.gateway_market_data_append_only_event_types
                if event_type.strip()
            ),
        )
        if not self.gateway_market_data_append_only_event_types:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_EVENT_TYPES must not be empty"
            )
        object.__setattr__(
            self,
            "gateway_market_data_append_only_cutover_event_types",
            tuple(
                event_type.strip().lower()
                for event_type in self.gateway_market_data_append_only_cutover_event_types
                if event_type.strip()
            ),
        )
        if self.gateway_market_data_append_only_price_tick_max_skip_per_minute < 0:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE "
                "must be >= 0"
            )
        if self.gateway_market_data_append_only_tr_response_max_skip_per_minute < 0:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE "
                "must be >= 0"
            )
        if self.gateway_market_data_append_only_condition_event_max_skip_per_minute < 0:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE "
                "must be >= 0"
            )
        if self.gateway_market_data_append_only_condition_event_max_payload_age_sec < 1:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_PAYLOAD_AGE_SEC "
                "must be >= 1"
            )
        if self.gateway_market_data_append_only_tr_response_max_rows_per_event < 1:
            raise ValueError(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_ROWS_PER_EVENT "
                "must be >= 1"
            )
        object.__setattr__(
            self,
            "gateway_market_data_append_only_min_outbox_status",
            _normalize_non_empty(
                self.gateway_market_data_append_only_min_outbox_status
            ).upper(),
        )
        if self.gateway_market_reference_append_only_reconcile_max_age_sec < 1:
            raise ValueError(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_RECONCILE_MAX_AGE_SEC "
                "must be >= 1"
            )
        if self.gateway_market_reference_append_only_max_skip_per_minute < 0:
            raise ValueError(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE "
                "must be >= 0"
            )
        if self.gateway_market_reference_append_only_max_pending_within_sla < 1:
            raise ValueError(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA "
                "must be >= 1"
            )
        if self.gateway_market_reference_append_only_min_membership_count < 0:
            raise ValueError(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT "
                "must be >= 0"
            )
        if self.gateway_market_index_append_only_reconcile_max_age_sec < 1:
            raise ValueError(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_RECONCILE_MAX_AGE_SEC must be >= 1"
            )
        if self.gateway_market_index_append_only_max_skip_per_minute < 0:
            raise ValueError(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE must be >= 0"
            )
        if self.gateway_market_index_append_only_max_pending_within_sla < 1:
            raise ValueError(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_PENDING_WITHIN_SLA must be >= 1"
            )
        if self.gateway_market_index_append_only_max_event_age_sec < 1:
            raise ValueError(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_EVENT_AGE_SEC must be >= 1"
            )
        if self.gateway_market_index_append_only_max_future_skew_sec < 0:
            raise ValueError(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_FUTURE_SKEW_SEC must be >= 0"
            )
        if self.gateway_market_index_append_only_gateway_health_max_age_sec < 1:
            raise ValueError(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_GATEWAY_HEALTH_MAX_AGE_SEC must be >= 1"
            )
        if self.gateway_market_regime_append_only_reconcile_max_age_sec < 1:
            raise ValueError(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_RECONCILE_MAX_AGE_SEC must be >= 1"
            )
        if self.gateway_market_regime_append_only_max_skip_per_minute < 0:
            raise ValueError(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE must be >= 0"
            )
        if self.gateway_market_regime_append_only_max_pending_within_sla < 1:
            raise ValueError(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_PENDING_WITHIN_SLA must be >= 1"
            )
        if self.market_index_stale_sec < 1:
            raise ValueError("MARKET_INDEX_STALE_SEC must be >= 1")
        if self.market_context_snapshot_stale_sec < 1:
            raise ValueError("MARKET_CONTEXT_SNAPSHOT_STALE_SEC must be >= 1")
        if self.market_scan_interval_sec < 1:
            raise ValueError("MARKET_SCAN_INTERVAL_SEC must be >= 1")
        if self.market_scan_top_n < 1:
            raise ValueError("MARKET_SCAN_TOP_N must be >= 1")
        object.__setattr__(
            self,
            "market_scan_tr_codes",
            _normalize_required_mapping(
                self.market_scan_tr_codes,
                "MARKET_SCAN_TR_CODES",
                required_keys=("TRADE_VALUE", "CHANGE_RATE"),
            ),
        )
        object.__setattr__(
            self,
            "market_scan_markets",
            _normalize_market_scan_markets(self.market_scan_markets),
        )
        object.__setattr__(
            self,
            "market_scan_market_codes",
            _normalize_required_mapping(
                self.market_scan_market_codes,
                "MARKET_SCAN_MARKET_CODES",
                required_keys=self.market_scan_markets,
            ),
        )
        object.__setattr__(
            self,
            "market_scan_screen_no",
            _require_non_empty_config(self.market_scan_screen_no),
        )
        object.__setattr__(
            self,
            "market_scan_parser_status",
            _normalize_non_empty(self.market_scan_parser_status),
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
        if self.theme_min_observable_members < 1:
            raise ValueError("THEME_MIN_OBSERVABLE_MEMBERS must be >= 1")
        if self.theme_snapshot_max_members < 1:
            raise ValueError("THEME_SNAPSHOT_MAX_MEMBERS must be >= 1")
        if self.theme_snapshot_stale_sec < 1:
            raise ValueError("THEME_SNAPSHOT_STALE_SEC must be >= 1")
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
        for field_name in (
            "realtime_subscription_max_total",
            "realtime_subscription_max_per_theme",
            "realtime_subscription_stale_sec",
            "realtime_subscription_remove_stale_after_sec",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.realtime_subscription_remove_stale_after_sec < self.realtime_subscription_stale_sec:
            raise ValueError(
                "REALTIME_SUBSCRIPTION_REMOVE_STALE_AFTER_SEC must be >= "
                "REALTIME_SUBSCRIPTION_STALE_SEC"
            )
        if self.realtime_subscription_max_per_theme > self.realtime_subscription_max_total:
            raise ValueError(
                "REALTIME_SUBSCRIPTION_MAX_PER_THEME must be <= REALTIME_SUBSCRIPTION_MAX_TOTAL"
            )
        object.__setattr__(
            self,
            "realtime_subscription_anchor_codes",
            _normalize_stock_code_list(self.realtime_subscription_anchor_codes),
        )
        if len(self.realtime_subscription_anchor_codes) > self.realtime_subscription_max_total:
            raise ValueError(
                "REALTIME_SUBSCRIPTION_MAX_TOTAL must be >= "
                "the number of REALTIME_SUBSCRIPTION_ANCHOR_CODES"
            )
        object.__setattr__(
            self,
            "realtime_subscription_exchange",
            _normalize_non_empty(self.realtime_subscription_exchange),
        )
        if self.realtime_subscription_exchange not in {"KRX", "NXT", "ALL"}:
            raise ValueError("REALTIME_SUBSCRIPTION_EXCHANGE must be one of KRX, NXT, ALL")
        if self.condition_fusion_sweep_interval_sec < 1:
            raise ValueError("CONDITION_FUSION_SWEEP_INTERVAL_SEC must be >= 1")
        if self.incremental_evaluation_worker_interval_sec <= 0:
            raise ValueError("INCREMENTAL_EVALUATION_WORKER_INTERVAL_SEC must be > 0")
        for field_name in (
            "incremental_evaluation_batch_size",
            "incremental_evaluation_retry_limit",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.projection_outbox_worker_interval_sec <= 0:
            raise ValueError("PROJECTION_OUTBOX_WORKER_INTERVAL_SEC must be > 0")
        for field_name in (
            "projection_outbox_batch_size",
            "projection_outbox_apply_batch_size",
            "projection_outbox_market_reference_apply_batch_size",
            "projection_outbox_market_index_apply_batch_size",
            "projection_outbox_market_regime_apply_batch_size",
            "projection_outbox_live_run_once_batch_size",
            "projection_outbox_run_once_max_wall_ms",
            "projection_outbox_retry_limit",
            "projection_outbox_processing_ttl_sec",
            "projection_outbox_backlog_warn_pending_count",
            "projection_outbox_backlog_fail_pending_count",
            "projection_outbox_backlog_recent_window_sec",
            "projection_outbox_backlog_recent_fail_count",
            "projection_outbox_backlog_stale_processing_sec",
            "projection_outbox_backlog_condition_event_ready_max_pending",
            "projection_outbox_backlog_condition_event_ready_recent_max_pending",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.projection_outbox_shadow_min_age_sec < 0:
            raise ValueError("PROJECTION_OUTBOX_SHADOW_MIN_AGE_SEC must be >= 0")
        if self.projection_outbox_apply_min_age_sec < 0:
            raise ValueError("PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC must be >= 0")
        if self.projection_outbox_market_reference_apply_min_age_sec < 0:
            raise ValueError(
                "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_MIN_AGE_SEC must be >= 0"
            )
        if self.projection_outbox_market_index_apply_min_age_sec < 0:
            raise ValueError(
                "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_MIN_AGE_SEC must be >= 0"
            )
        if self.projection_outbox_market_regime_apply_min_age_sec < 0:
            raise ValueError(
                "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC must be >= 0"
            )
        if not self.projection_outbox_shadow_mode:
            raise ValueError("PROJECTION_OUTBOX_SHADOW_MODE must remain true")
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
            "risk_cross_exchange_divergence_bp",
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
            "live_sim_max_daily_loss",
            "live_sim_max_daily_loss_pct",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name.upper()} must be >= 0")
        for field_name in (
            "live_sim_max_daily_order_count",
            "live_sim_max_active_orders",
            "live_sim_max_active_positions",
            "live_sim_duplicate_cooldown_sec",
            "live_sim_order_ttl_sec",
            "live_sim_preflight_pending_command_backlog_warn_threshold",
            "live_sim_stale_tick_sec",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.live_sim_max_daily_notional < self.live_sim_max_order_notional:
            raise ValueError("LIVE_SIM_MAX_DAILY_NOTIONAL must be >= LIVE_SIM_MAX_ORDER_NOTIONAL")
        if self.live_sim_price_offset_ticks < 0:
            raise ValueError("LIVE_SIM_PRICE_OFFSET_TICKS must be >= 0")
        if self.live_sim_buy_price_offset_ticks < 0:
            raise ValueError("LIVE_SIM_BUY_PRICE_OFFSET_TICKS must be >= 0")
        if self.live_sim_buy_price_offset_ticks > 3:
            raise ValueError("LIVE_SIM_BUY_PRICE_OFFSET_TICKS must be <= 3")
        if self.live_sim_reprice_max_attempts < 1:
            raise ValueError("LIVE_SIM_REPRICE_MAX_ATTEMPTS must be >= 1")
        object.__setattr__(
            self,
            "live_sim_entry_window_start",
            _validate_time_string(
                self.live_sim_entry_window_start,
                "LIVE_SIM_ENTRY_WINDOW_START",
            ),
        )
        object.__setattr__(
            self,
            "live_sim_entry_window_end",
            _validate_time_string(
                self.live_sim_entry_window_end,
                "LIVE_SIM_ENTRY_WINDOW_END",
            ),
        )
        if self.live_sim_entry_window_start >= self.live_sim_entry_window_end:
            raise ValueError("LIVE_SIM_ENTRY_WINDOW_START must be < LIVE_SIM_ENTRY_WINDOW_END")
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
            "live_sim_order_exchange",
            _normalize_live_sim_order_exchange(self.live_sim_order_exchange),
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
        object.__setattr__(
            self,
            "live_sim_exit_eod_flatten_time",
            _validate_time_string(
                self.live_sim_exit_eod_flatten_time,
                "LIVE_SIM_EXIT_EOD_FLATTEN_TIME",
            ),
        )
        if self.live_sim_entry_window_end >= self.live_sim_exit_eod_flatten_time:
            raise ValueError(
                "LIVE_SIM_ENTRY_WINDOW_END must be < LIVE_SIM_EXIT_EOD_FLATTEN_TIME"
            )
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
        if self.live_sim_reconcile_notional_tolerance < 0:
            raise ValueError("LIVE_SIM_RECONCILE_NOTIONAL_TOLERANCE must be >= 0")
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
        if self.live_sim_operating_loop_interval_sec < 5:
            raise ValueError("LIVE_SIM_OPERATING_LOOP_INTERVAL_SEC must be >= 5")
        object.__setattr__(
            self,
            "live_sim_operating_loop_market_open_time",
            _validate_time_string(
                self.live_sim_operating_loop_market_open_time,
                "LIVE_SIM_OPERATING_LOOP_MARKET_OPEN_TIME",
            ),
        )
        object.__setattr__(
            self,
            "live_sim_operating_loop_market_close_time",
            _validate_time_string(
                self.live_sim_operating_loop_market_close_time,
                "LIVE_SIM_OPERATING_LOOP_MARKET_CLOSE_TIME",
            ),
        )
        for field_name in ("dashboard_refresh_sec", "dashboard_snapshot_default_limit"):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name.upper()} must be >= 1")
        if self.dashboard_max_limit < 1:
            raise ValueError("DASHBOARD_MAX_LIMIT must be >= 1")
        if self.dashboard_snapshot_default_limit > self.dashboard_max_limit:
            raise ValueError("DASHBOARD_SNAPSHOT_DEFAULT_LIMIT must be <= DASHBOARD_MAX_LIMIT")
        if self.dashboard_snapshot_fast_cache_ttl_sec < 0:
            raise ValueError("DASHBOARD_SNAPSHOT_FAST_CACHE_TTL_SEC must be >= 0")
        if self.dashboard_snapshot_fast_default_limit < 1:
            raise ValueError("DASHBOARD_SNAPSHOT_FAST_DEFAULT_LIMIT must be >= 1")
        if self.dashboard_snapshot_fast_timeout_budget_ms < 100:
            raise ValueError("DASHBOARD_SNAPSHOT_FAST_TIMEOUT_BUDGET_MS must be >= 100")
        if self.dashboard_snapshot_warn_latency_ms < 1:
            raise ValueError("DASHBOARD_SNAPSHOT_WARN_LATENCY_MS must be >= 1")
        if self.dashboard_snapshot_fail_latency_ms < self.dashboard_snapshot_warn_latency_ms:
            raise ValueError(
                "DASHBOARD_SNAPSHOT_FAIL_LATENCY_MS must be >= "
                "DASHBOARD_SNAPSHOT_WARN_LATENCY_MS"
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
        if self.event_store_retention_days < 1:
            raise ValueError("EVENT_STORE_RETENTION_DAYS must be >= 1")
        if self.event_store_retention_batch_size < 1:
            raise ValueError("EVENT_STORE_RETENTION_BATCH_SIZE must be >= 1")
        if self.event_store_retention_interval_sec < 60:
            raise ValueError("EVENT_STORE_RETENTION_INTERVAL_SEC must be >= 60")
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
    def trading_capabilities(self) -> TradingCapabilities:
        return _TRADING_CAPABILITY_MATRIX[self.trading_profile]

    @property
    def deprecated_flag_warning_dicts(self) -> tuple[dict[str, str], ...]:
        return tuple(item.to_dict() for item in self.deprecated_flag_warnings)

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
    if environ is None:
        return _load_default_settings()
    return _build_settings(environ)


def clear_settings_cache() -> None:
    _load_default_settings.cache_clear()


@lru_cache(maxsize=1)
def _load_default_settings() -> Settings:
    """Load settings from os.environ overlaid by .env file values.

    The .env file has higher priority than os.environ so intraday operator
    edits use .env as the single source of truth.
    """
    return _build_settings(_default_settings_environment())


def _default_settings_environment() -> dict[str, str]:
    env = dict(os.environ)
    env.update(_load_env_file_values(_resolve_env_file_path(env)))
    return env


def _resolve_env_file_path(env: Mapping[str, str]) -> Path:
    configured_path = env.get(ENV_FILE_PATH_ENV, "").strip()
    if configured_path:
        return Path(configured_path).expanduser()
    return DEFAULT_ENV_FILE_PATH


def _load_env_file_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}

    values: dict[str, str] = {}
    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        separator = line.find("=")
        if separator < 1:
            continue

        name = line[:separator].strip()
        value = line[separator + 1 :].strip()
        if len(value) >= 2 and (
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = value[1:-1]
        values[name] = value

    return values


def _build_settings(env: Mapping[str, str]) -> Settings:

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
        market_data_premarket_snapshot_enabled=_parse_bool(
            env.get("MARKET_DATA_PREMARKET_SNAPSHOT_ENABLED", "false")
        ),
        market_data_projection_reconcile_limit=_parse_int(
            env.get("MARKET_DATA_PROJECTION_RECONCILE_LIMIT", "500"),
            "MARKET_DATA_PROJECTION_RECONCILE_LIMIT",
            min_value=1,
        ),
        market_data_reconcile_live_default_persist=_parse_bool(
            env.get("MARKET_DATA_RECONCILE_LIVE_DEFAULT_PERSIST", "false")
        ),
        market_data_reconcile_locked_fallback_to_read_only=_parse_bool(
            env.get("MARKET_DATA_RECONCILE_LOCKED_FALLBACK_TO_READ_ONLY", "true")
        ),
        operator_sqlite_lock_retry_attempts=_parse_int(
            env.get("OPERATOR_SQLITE_LOCK_RETRY_ATTEMPTS", "3"),
            "OPERATOR_SQLITE_LOCK_RETRY_ATTEMPTS",
            min_value=1,
        ),
        operator_sqlite_lock_retry_base_sleep_sec=_parse_float(
            env.get("OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC", "0.05"),
            "OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC",
            min_value=0.0,
        ),
        operator_sqlite_lock_retry_max_sleep_sec=_parse_float(
            env.get("OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC", "0.5"),
            "OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC",
            min_value=0.0,
        ),
        operator_sqlite_busy_timeout_ms=_parse_int(
            env.get("OPERATOR_SQLITE_BUSY_TIMEOUT_MS", "500"),
            "OPERATOR_SQLITE_BUSY_TIMEOUT_MS",
            min_value=1,
        ),
        operator_run_once_locked_http_status=_parse_int(
            env.get("OPERATOR_RUN_ONCE_LOCKED_HTTP_STATUS", "409"),
            "OPERATOR_RUN_ONCE_LOCKED_HTTP_STATUS",
            min_value=1,
        ),
        ops_script_locked_retry_attempts=_parse_int(
            env.get("OPS_SCRIPT_LOCKED_RETRY_ATTEMPTS", "3"),
            "OPS_SCRIPT_LOCKED_RETRY_ATTEMPTS",
            min_value=1,
        ),
        ops_script_locked_retry_sleep_sec=_parse_float(
            env.get("OPS_SCRIPT_LOCKED_RETRY_SLEEP_SEC", "1.0"),
            "OPS_SCRIPT_LOCKED_RETRY_SLEEP_SEC",
            min_value=0.0,
        ),
        gateway_market_data_append_only_dry_run_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED", "false")
        ),
        gateway_market_data_append_only_cutover_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED", "false")
        ),
        gateway_market_data_append_only_operating_mode=env.get(
            "GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE",
            "OFF",
        ),
        gateway_market_data_append_only_global_kill_switch=_parse_bool(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH", "true")
        ),
        gateway_market_data_append_only_auto_rollback_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_ENABLED", "true")
        ),
        gateway_market_data_append_only_global_max_skip_per_minute=_parse_int(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE", "0"),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE",
            min_value=0,
        ),
        gateway_market_data_append_only_max_error_count=_parse_int(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_ERROR_COUNT", "0"),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_ERROR_COUNT",
            min_value=0,
        ),
        gateway_market_data_append_only_max_dead_letter_count=_parse_int(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_DEAD_LETTER_COUNT", "0"),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_DEAD_LETTER_COUNT",
            min_value=0,
        ),
        gateway_market_data_append_only_max_pending_within_sla=_parse_int(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_PENDING_WITHIN_SLA", "100"),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_PENDING_WITHIN_SLA",
            min_value=0,
        ),
        gateway_market_data_append_only_max_condition_event_pending_within_sla=(
            _parse_int(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_CONDITION_EVENT_PENDING_WITHIN_SLA",
                    "10",
                ),
                "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_CONDITION_EVENT_PENDING_WITHIN_SLA",
                min_value=0,
            )
        ),
        gateway_market_data_append_only_require_dashboard_fast_ok=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_DASHBOARD_FAST_OK",
                "true",
            )
        ),
        gateway_market_data_append_only_require_backlog_ready=_parse_bool(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_BACKLOG_READY", "true")
        ),
        gateway_market_data_append_only_auto_rollback_cooldown_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_COOLDOWN_SEC",
                "300",
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_COOLDOWN_SEC",
            min_value=1,
        ),
        gateway_market_data_append_only_health_stale_sec=_parse_int(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_HEALTH_STALE_SEC", "60"),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_HEALTH_STALE_SEC",
            min_value=1,
        ),
        gateway_market_data_append_only_price_tick_cutover_enabled=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED",
                "false",
            )
        ),
        gateway_market_data_append_only_tr_response_dry_run_enabled=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_DRY_RUN_ENABLED",
                "false",
            )
        ),
        gateway_market_data_append_only_tr_response_cutover_enabled=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED",
                "false",
            )
        ),
        gateway_market_data_append_only_tr_response_require_worker_side_effects=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_REQUIRE_WORKER_SIDE_EFFECTS",
                    "true",
                )
            )
        ),
        gateway_market_data_append_only_tr_response_max_skip_per_minute=_parse_int(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE",
                "0",
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE",
            min_value=0,
        ),
        gateway_market_data_append_only_tr_response_require_synthetic_child_guard=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_REQUIRE_SYNTHETIC_CHILD_GUARD",
                    "true",
                )
            )
        ),
        gateway_market_data_append_only_tr_response_max_rows_per_event=_parse_int(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_ROWS_PER_EVENT",
                "50",
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_ROWS_PER_EVENT",
            min_value=1,
        ),
        gateway_market_data_append_only_tr_response_fail_closed_on_side_effect_error=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_FAIL_CLOSED_ON_SIDE_EFFECT_ERROR",
                    "true",
                )
            )
        ),
        gateway_market_data_append_only_condition_event_dry_run_enabled=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_DRY_RUN_ENABLED",
                "false",
            )
        ),
        gateway_market_data_append_only_condition_event_cutover_enabled=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED",
                "false",
            )
        ),
        gateway_market_data_append_only_condition_event_require_worker_side_effects=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_REQUIRE_WORKER_SIDE_EFFECTS",
                    "true",
                )
            )
        ),
        gateway_market_data_append_only_condition_event_require_fusion_enabled=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_REQUIRE_FUSION_ENABLED",
                    "true",
                )
            )
        ),
        gateway_market_data_append_only_condition_event_require_backlog_ready=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_REQUIRE_BACKLOG_READY",
                    "true",
                )
            )
        ),
        gateway_market_data_append_only_condition_event_max_skip_per_minute=_parse_int(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE",
                "0",
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE",
            min_value=0,
        ),
        gateway_market_data_append_only_condition_event_fail_closed_on_side_effect_error=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_FAIL_CLOSED_ON_SIDE_EFFECT_ERROR",
                    "true",
                )
            )
        ),
        gateway_market_data_append_only_condition_event_allow_candidate_ingest_in_worker=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_ALLOW_CANDIDATE_INGEST_IN_WORKER",
                    "false",
                )
            )
        ),
        gateway_market_data_append_only_condition_event_max_payload_age_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_PAYLOAD_AGE_SEC",
                "60",
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_PAYLOAD_AGE_SEC",
            min_value=1,
        ),
        gateway_market_data_append_only_cutover_event_types=tuple(
            _parse_csv_list(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_EVENT_TYPES",
                    "price_tick",
                ),
                "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_EVENT_TYPES",
            )
        ),
        gateway_market_data_append_only_require_reconcile_pass=_parse_bool(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_RECONCILE_PASS", "true")
        ),
        gateway_market_data_append_only_require_latest_reconcile_pass=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_LATEST_RECONCILE_PASS",
                "true",
            )
        ),
        gateway_market_data_append_only_require_worker_apply_enabled=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_WORKER_APPLY_ENABLED",
                "true",
            )
        ),
        gateway_market_data_append_only_fail_closed_on_routing_error=_parse_bool(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_FAIL_CLOSED_ON_ROUTING_ERROR",
                "true",
            )
        ),
        gateway_market_data_append_only_price_tick_max_skip_per_minute=_parse_int(
            env.get(
                "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE",
                "0",
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE",
            min_value=0,
        ),
        gateway_market_data_append_only_reconcile_max_age_sec=_parse_int(
            env.get("GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC", "300"),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC",
            min_value=1,
        ),
        gateway_market_data_append_only_event_types=tuple(
            _parse_csv_list(
                env.get(
                    "GATEWAY_MARKET_DATA_APPEND_ONLY_EVENT_TYPES",
                    "price_tick,condition_event,tr_response",
                ),
                "GATEWAY_MARKET_DATA_APPEND_ONLY_EVENT_TYPES",
            )
        ),
        gateway_market_data_append_only_min_outbox_status=env.get(
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MIN_OUTBOX_STATUS",
            "ENQUEUED",
        ),
        gateway_market_reference_append_only_dry_run_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED", "false")
        ),
        gateway_market_reference_append_only_cutover_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED", "false")
        ),
        gateway_market_reference_append_only_global_kill_switch=_parse_bool(
            env.get(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH",
                "true",
            )
        ),
        gateway_market_reference_append_only_max_skip_per_minute=_parse_int(
            env.get(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
                "0",
            ),
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
            min_value=0,
        ),
        gateway_market_reference_append_only_max_pending_within_sla=_parse_int(
            env.get(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA",
                "1",
            ),
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA",
            min_value=1,
        ),
        gateway_market_reference_append_only_require_reconcile_pass=_parse_bool(
            env.get(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_REQUIRE_RECONCILE_PASS",
                "true",
            )
        ),
        gateway_market_reference_append_only_reconcile_max_age_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_RECONCILE_MAX_AGE_SEC",
                "300",
            ),
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_RECONCILE_MAX_AGE_SEC",
            min_value=1,
        ),
        gateway_market_reference_append_only_min_membership_count=_parse_int(
            env.get(
                "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT",
                "100",
            ),
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT",
            min_value=0,
        ),
        gateway_market_reference_append_only_effective_skip_disabled_in_pr13=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13",
                    "true",
                )
            )
        ),
        gateway_market_index_append_only_dry_run_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED", "false")
        ),
        gateway_market_index_append_only_cutover_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED", "false")
        ),
        gateway_market_index_append_only_global_kill_switch=_parse_bool(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH",
                "true",
            )
        ),
        gateway_market_index_append_only_max_skip_per_minute=_parse_int(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
                "0",
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
            min_value=0,
        ),
        gateway_market_index_append_only_max_pending_within_sla=_parse_int(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_PENDING_WITHIN_SLA",
                "1",
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_PENDING_WITHIN_SLA",
            min_value=1,
        ),
        gateway_market_index_append_only_require_reconcile_pass=_parse_bool(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_RECONCILE_PASS",
                "true",
            )
        ),
        gateway_market_index_append_only_require_data_usable=_parse_bool(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_DATA_USABLE",
                "true",
            )
        ),
        gateway_market_index_append_only_require_parser_verified=_parse_bool(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_PARSER_VERIFIED",
                "true",
            )
        ),
        gateway_market_index_append_only_require_worker_regime_refresh=_parse_bool(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_WORKER_REGIME_REFRESH",
                "true",
            )
        ),
        gateway_market_index_append_only_fail_closed_on_regime_refresh_error=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_INDEX_APPEND_ONLY_FAIL_CLOSED_ON_REGIME_REFRESH_ERROR",
                    "true",
                )
            )
        ),
        gateway_market_index_append_only_reconcile_max_age_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_RECONCILE_MAX_AGE_SEC",
                "300",
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_RECONCILE_MAX_AGE_SEC",
            min_value=1,
        ),
        gateway_market_index_append_only_max_event_age_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_EVENT_AGE_SEC",
                "30",
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_EVENT_AGE_SEC",
            min_value=1,
        ),
        gateway_market_index_append_only_max_future_skew_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_FUTURE_SKEW_SEC",
                "5",
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_FUTURE_SKEW_SEC",
            min_value=0,
        ),
        gateway_market_index_append_only_require_fresh_gateway_health=_parse_bool(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_FRESH_GATEWAY_HEALTH",
                "true",
            )
        ),
        gateway_market_index_append_only_gateway_health_max_age_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_INDEX_APPEND_ONLY_GATEWAY_HEALTH_MAX_AGE_SEC",
                "30",
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_GATEWAY_HEALTH_MAX_AGE_SEC",
            min_value=1,
        ),
        gateway_market_index_append_only_effective_skip_disabled_in_pr15=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15",
                    "true",
                )
            )
        ),
        gateway_market_regime_append_only_dry_run_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED", "false")
        ),
        gateway_market_regime_append_only_cutover_enabled=_parse_bool(
            env.get("GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED", "false")
        ),
        gateway_market_regime_append_only_global_kill_switch=_parse_bool(
            env.get("GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH", "true")
        ),
        gateway_market_regime_append_only_max_skip_per_minute=_parse_int(
            env.get("GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE", "0"),
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
            min_value=0,
        ),
        gateway_market_regime_append_only_max_pending_within_sla=_parse_int(
            env.get("GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_PENDING_WITHIN_SLA", "1"),
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_PENDING_WITHIN_SLA",
            min_value=1,
        ),
        gateway_market_regime_append_only_require_reconcile_pass=_parse_bool(
            env.get(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_RECONCILE_PASS",
                "true",
            )
        ),
        gateway_market_regime_append_only_require_prior_event_reconcile=_parse_bool(
            env.get(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_PRIOR_EVENT_RECONCILE",
                "true",
            )
        ),
        gateway_market_regime_append_only_require_index_routing_guard=_parse_bool(
            env.get(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_INDEX_ROUTING_GUARD",
                "true",
            )
        ),
        gateway_market_regime_append_only_require_worker_context_refresh=_parse_bool(
            env.get(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_WORKER_CONTEXT_REFRESH",
                "true",
            )
        ),
        gateway_market_regime_append_only_fail_closed_on_context_refresh_error=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_REGIME_APPEND_ONLY_FAIL_CLOSED_ON_CONTEXT_REFRESH_ERROR",
                    "true",
                )
            )
        ),
        gateway_market_regime_append_only_reconcile_max_age_sec=_parse_int(
            env.get(
                "GATEWAY_MARKET_REGIME_APPEND_ONLY_RECONCILE_MAX_AGE_SEC",
                "300",
            ),
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_RECONCILE_MAX_AGE_SEC",
            min_value=1,
        ),
        gateway_market_regime_append_only_effective_skip_disabled_in_pr18=(
            _parse_bool(
                env.get(
                    "GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18",
                    "true",
                )
            )
        ),
        projection_event_result_backfill_enabled=_parse_bool(
            env.get("PROJECTION_EVENT_RESULT_BACKFILL_ENABLED", "false")
        ),
        event_store_retention_enabled=_parse_bool(
            env.get("EVENT_STORE_RETENTION_ENABLED", "false")
        ),
        event_store_retention_days=_parse_int(
            env.get("EVENT_STORE_RETENTION_DAYS", "30"),
            "EVENT_STORE_RETENTION_DAYS",
            min_value=1,
        ),
        event_store_retention_batch_size=_parse_int(
            env.get("EVENT_STORE_RETENTION_BATCH_SIZE", "5000"),
            "EVENT_STORE_RETENTION_BATCH_SIZE",
            min_value=1,
        ),
        event_store_retention_interval_sec=_parse_int(
            env.get("EVENT_STORE_RETENTION_INTERVAL_SEC", "86400"),
            "EVENT_STORE_RETENTION_INTERVAL_SEC",
            min_value=60,
        ),
        market_regime_enabled=_parse_bool(env.get("MARKET_REGIME_ENABLED", "true")),
        market_context_snapshot_stale_sec=_parse_int(
            env.get("MARKET_CONTEXT_SNAPSHOT_STALE_SEC", "30"),
            "MARKET_CONTEXT_SNAPSHOT_STALE_SEC",
            min_value=1,
        ),
        market_index_stale_sec=_parse_int(
            env.get("MARKET_INDEX_STALE_SEC", "30"),
            "MARKET_INDEX_STALE_SEC",
            min_value=1,
        ),
        market_scan_enabled=_parse_bool(env.get("MARKET_SCAN_ENABLED", "false")),
        market_scan_interval_sec=_parse_int(
            env.get("MARKET_SCAN_INTERVAL_SEC", "120"),
            "MARKET_SCAN_INTERVAL_SEC",
            min_value=1,
        ),
        market_scan_top_n=_parse_int(
            env.get("MARKET_SCAN_TOP_N", "200"),
            "MARKET_SCAN_TOP_N",
            min_value=1,
        ),
        market_scan_tr_codes=_parse_key_value_mapping(
            env.get(
                "MARKET_SCAN_TR_CODES",
                "TRADE_VALUE=OPT10032,CHANGE_RATE=OPT10027",
            ),
            "MARKET_SCAN_TR_CODES",
        ),
        market_scan_markets=_parse_csv_list(
            env.get("MARKET_SCAN_MARKETS", "KOSPI,KOSDAQ"),
            "MARKET_SCAN_MARKETS",
        ),
        market_scan_market_codes=_parse_key_value_mapping(
            env.get("MARKET_SCAN_MARKET_CODES", "KOSPI=001,KOSDAQ=101"),
            "MARKET_SCAN_MARKET_CODES",
        ),
        market_scan_screen_no=env.get("MARKET_SCAN_SCREEN_NO", "8800"),
        market_scan_parser_status=env.get(
            "MARKET_SCAN_PARSER_STATUS",
            "PILOT_UNVERIFIED",
        ),
        market_regime_risk_on_return_5m=_parse_float(
            env.get("MARKET_REGIME_RISK_ON_RETURN_5M", "0.15"),
            "MARKET_REGIME_RISK_ON_RETURN_5M",
        ),
        market_regime_weak_drawdown_15m=_parse_float(
            env.get("MARKET_REGIME_WEAK_DRAWDOWN_15M", "-0.40"),
            "MARKET_REGIME_WEAK_DRAWDOWN_15M",
        ),
        market_regime_risk_off_return_5m=_parse_float(
            env.get("MARKET_REGIME_RISK_OFF_RETURN_5M", "-0.35"),
            "MARKET_REGIME_RISK_OFF_RETURN_5M",
        ),
        market_regime_risk_off_drawdown_15m=_parse_float(
            env.get("MARKET_REGIME_RISK_OFF_DRAWDOWN_15M", "-0.80"),
            "MARKET_REGIME_RISK_OFF_DRAWDOWN_15M",
        ),
        market_regime_secondary_risk_off_return_5m=_parse_float(
            env.get("MARKET_REGIME_SECONDARY_RISK_OFF_RETURN_5M", "-0.60"),
            "MARKET_REGIME_SECONDARY_RISK_OFF_RETURN_5M",
        ),
        realtime_subscription_enabled=_parse_bool(
            env.get("REALTIME_SUBSCRIPTION_ENABLED", "true")
        ),
        realtime_subscription_queue_commands=_parse_bool(
            env.get("REALTIME_SUBSCRIPTION_QUEUE_COMMANDS", "false")
        ),
        realtime_subscription_max_total=_parse_int(
            env.get("REALTIME_SUBSCRIPTION_MAX_TOTAL", "50"),
            "REALTIME_SUBSCRIPTION_MAX_TOTAL",
            min_value=1,
        ),
        realtime_subscription_max_per_theme=_parse_int(
            env.get("REALTIME_SUBSCRIPTION_MAX_PER_THEME", "5"),
            "REALTIME_SUBSCRIPTION_MAX_PER_THEME",
            min_value=1,
        ),
        realtime_subscription_anchor_codes=_parse_stock_code_csv_list(
            env.get("REALTIME_SUBSCRIPTION_ANCHOR_CODES", "005930,000660"),
            "REALTIME_SUBSCRIPTION_ANCHOR_CODES",
        ),
        realtime_subscription_stale_sec=_parse_int(
            env.get("REALTIME_SUBSCRIPTION_STALE_SEC", "60"),
            "REALTIME_SUBSCRIPTION_STALE_SEC",
            min_value=1,
        ),
        realtime_subscription_remove_stale_after_sec=_parse_int(
            env.get("REALTIME_SUBSCRIPTION_REMOVE_STALE_AFTER_SEC", "600"),
            "REALTIME_SUBSCRIPTION_REMOVE_STALE_AFTER_SEC",
            min_value=1,
        ),
        realtime_subscription_allow_remove=_parse_bool(
            env.get("REALTIME_SUBSCRIPTION_ALLOW_REMOVE", "false")
        ),
        realtime_subscription_exchange=env.get(
            "REALTIME_SUBSCRIPTION_EXCHANGE",
            env.get("KIWOOM_REALTIME_EXCHANGE", "KRX"),
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
        theme_observable_coverage_enabled=_parse_bool(
            env.get("THEME_OBSERVABLE_COVERAGE_ENABLED", "true")
        ),
        theme_min_observable_members=_parse_int(
            env.get("THEME_MIN_OBSERVABLE_MEMBERS", "3"),
            "THEME_MIN_OBSERVABLE_MEMBERS",
            min_value=1,
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
        theme_snapshot_stale_sec=_parse_int(
            env.get("THEME_SNAPSHOT_STALE_SEC", "300"),
            "THEME_SNAPSHOT_STALE_SEC",
            min_value=1,
        ),
        theme_premarket_observables_enabled=_parse_bool(
            env.get("THEME_PREMARKET_OBSERVABLES_ENABLED", "false")
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
            env.get("NAVER_THEME_IMPORT_MAX_THEMES", "500"),
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
        condition_fusion_event_incremental_enabled=_parse_bool(
            env.get("CONDITION_FUSION_EVENT_INCREMENTAL_ENABLED", "true")
        ),
        condition_fusion_sweep_enabled=_parse_bool(
            env.get("CONDITION_FUSION_SWEEP_ENABLED", "true")
        ),
        condition_fusion_sweep_interval_sec=_parse_int(
            env.get("CONDITION_FUSION_SWEEP_INTERVAL_SEC", "60"),
            "CONDITION_FUSION_SWEEP_INTERVAL_SEC",
            min_value=1,
        ),
        incremental_evaluation_enabled=_parse_bool(
            env.get("INCREMENTAL_EVALUATION_ENABLED", "true")
        ),
        incremental_evaluation_worker_enabled=_parse_bool(
            env.get("INCREMENTAL_EVALUATION_WORKER_ENABLED", "true")
        ),
        incremental_evaluation_worker_interval_sec=_parse_float(
            env.get("INCREMENTAL_EVALUATION_WORKER_INTERVAL_SEC", "1.0"),
            "INCREMENTAL_EVALUATION_WORKER_INTERVAL_SEC",
            min_value=0.1,
        ),
        incremental_evaluation_batch_size=_parse_int(
            env.get("INCREMENTAL_EVALUATION_BATCH_SIZE", "20"),
            "INCREMENTAL_EVALUATION_BATCH_SIZE",
            min_value=1,
        ),
        incremental_evaluation_retry_limit=_parse_int(
            env.get("INCREMENTAL_EVALUATION_RETRY_LIMIT", "3"),
            "INCREMENTAL_EVALUATION_RETRY_LIMIT",
            min_value=1,
        ),
        projection_outbox_worker_enabled=_parse_bool(
            env.get("PROJECTION_OUTBOX_WORKER_ENABLED", "false")
        ),
        projection_outbox_worker_interval_sec=_parse_float(
            env.get("PROJECTION_OUTBOX_WORKER_INTERVAL_SEC", "1.0"),
            "PROJECTION_OUTBOX_WORKER_INTERVAL_SEC",
            min_value=0.1,
        ),
        projection_outbox_batch_size=_parse_int(
            env.get("PROJECTION_OUTBOX_BATCH_SIZE", "100"),
            "PROJECTION_OUTBOX_BATCH_SIZE",
            min_value=1,
        ),
        projection_outbox_retry_limit=_parse_int(
            env.get("PROJECTION_OUTBOX_RETRY_LIMIT", "3"),
            "PROJECTION_OUTBOX_RETRY_LIMIT",
            min_value=1,
        ),
        projection_outbox_processing_ttl_sec=_parse_int(
            env.get("PROJECTION_OUTBOX_PROCESSING_TTL_SEC", "60"),
            "PROJECTION_OUTBOX_PROCESSING_TTL_SEC",
            min_value=1,
        ),
        projection_outbox_shadow_mode=_parse_bool(
            env.get("PROJECTION_OUTBOX_SHADOW_MODE", "true")
        ),
        projection_outbox_apply_projection_enabled=_parse_bool(
            env.get("PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED", "false")
        ),
        projection_outbox_market_data_apply_enabled=_parse_bool(
            env.get("PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED", "false")
        ),
        projection_outbox_market_reference_apply_enabled=_parse_bool(
            env.get("PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED", "false")
        ),
        projection_outbox_market_index_apply_enabled=_parse_bool(
            env.get("PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED", "false")
        ),
        projection_outbox_market_regime_apply_enabled=_parse_bool(
            env.get("PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED", "false")
        ),
        projection_outbox_apply_batch_size=_parse_int(
            env.get("PROJECTION_OUTBOX_APPLY_BATCH_SIZE", "50"),
            "PROJECTION_OUTBOX_APPLY_BATCH_SIZE",
            min_value=1,
        ),
        projection_outbox_market_reference_apply_batch_size=_parse_int(
            env.get("PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_BATCH_SIZE", "20"),
            "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_BATCH_SIZE",
            min_value=1,
        ),
        projection_outbox_market_index_apply_batch_size=_parse_int(
            env.get("PROJECTION_OUTBOX_MARKET_INDEX_APPLY_BATCH_SIZE", "20"),
            "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_BATCH_SIZE",
            min_value=1,
        ),
        projection_outbox_market_regime_apply_batch_size=_parse_int(
            env.get("PROJECTION_OUTBOX_MARKET_REGIME_APPLY_BATCH_SIZE", "20"),
            "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_BATCH_SIZE",
            min_value=1,
        ),
        projection_outbox_live_run_once_batch_size=_parse_int(
            env.get("PROJECTION_OUTBOX_LIVE_RUN_ONCE_BATCH_SIZE", "50"),
            "PROJECTION_OUTBOX_LIVE_RUN_ONCE_BATCH_SIZE",
            min_value=1,
        ),
        projection_outbox_run_once_max_wall_ms=_parse_int(
            env.get("PROJECTION_OUTBOX_RUN_ONCE_MAX_WALL_MS", "5000"),
            "PROJECTION_OUTBOX_RUN_ONCE_MAX_WALL_MS",
            min_value=1,
        ),
        projection_outbox_apply_min_age_sec=_parse_float(
            env.get("PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC", "1.0"),
            "PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC",
            min_value=0.0,
        ),
        projection_outbox_market_reference_apply_min_age_sec=_parse_float(
            env.get("PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_MIN_AGE_SEC", "1.0"),
            "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_MIN_AGE_SEC",
            min_value=0.0,
        ),
        projection_outbox_market_index_apply_min_age_sec=_parse_float(
            env.get("PROJECTION_OUTBOX_MARKET_INDEX_APPLY_MIN_AGE_SEC", "1.0"),
            "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_MIN_AGE_SEC",
            min_value=0.0,
        ),
        projection_outbox_market_regime_apply_min_age_sec=_parse_float(
            env.get("PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC", "1.0"),
            "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC",
            min_value=0.0,
        ),
        projection_outbox_shadow_min_age_sec=_parse_float(
            env.get("PROJECTION_OUTBOX_SHADOW_MIN_AGE_SEC", "0.5"),
            "PROJECTION_OUTBOX_SHADOW_MIN_AGE_SEC",
            min_value=0.0,
        ),
        projection_outbox_backlog_warn_pending_count=_parse_int(
            env.get("PROJECTION_OUTBOX_BACKLOG_WARN_PENDING_COUNT", "1000"),
            "PROJECTION_OUTBOX_BACKLOG_WARN_PENDING_COUNT",
            min_value=1,
        ),
        projection_outbox_backlog_fail_pending_count=_parse_int(
            env.get("PROJECTION_OUTBOX_BACKLOG_FAIL_PENDING_COUNT", "10000"),
            "PROJECTION_OUTBOX_BACKLOG_FAIL_PENDING_COUNT",
            min_value=1,
        ),
        projection_outbox_backlog_recent_window_sec=_parse_int(
            env.get("PROJECTION_OUTBOX_BACKLOG_RECENT_WINDOW_SEC", "300"),
            "PROJECTION_OUTBOX_BACKLOG_RECENT_WINDOW_SEC",
            min_value=1,
        ),
        projection_outbox_backlog_recent_fail_count=_parse_int(
            env.get("PROJECTION_OUTBOX_BACKLOG_RECENT_FAIL_COUNT", "100"),
            "PROJECTION_OUTBOX_BACKLOG_RECENT_FAIL_COUNT",
            min_value=1,
        ),
        projection_outbox_backlog_stale_processing_sec=_parse_int(
            env.get("PROJECTION_OUTBOX_BACKLOG_STALE_PROCESSING_SEC", "120"),
            "PROJECTION_OUTBOX_BACKLOG_STALE_PROCESSING_SEC",
            min_value=1,
        ),
        projection_outbox_backlog_condition_event_ready_max_pending=_parse_int(
            env.get(
                "PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_MAX_PENDING",
                "100",
            ),
            "PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_MAX_PENDING",
            min_value=1,
        ),
        projection_outbox_backlog_condition_event_ready_recent_max_pending=_parse_int(
            env.get(
                "PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_RECENT_MAX_PENDING",
                "10",
            ),
            "PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_RECENT_MAX_PENDING",
            min_value=1,
        ),
        projection_outbox_backlog_required_for_condition_event_cutover=_parse_bool(
            env.get(
                "PROJECTION_OUTBOX_BACKLOG_REQUIRED_FOR_CONDITION_EVENT_CUTOVER",
                "true",
            )
        ),
        candidate_fsm_enabled=_parse_bool(env.get("CANDIDATE_FSM_ENABLED", "true")),
        candidate_trade_date_timezone=env.get("CANDIDATE_TRADE_DATE_TIMEZONE", "Asia/Seoul"),
        candidate_source_stale_sec=_parse_int(
            env.get("CANDIDATE_SOURCE_STALE_SEC", "300"),
            "CANDIDATE_SOURCE_STALE_SEC",
            min_value=1,
        ),
        candidate_tick_stale_sec=_parse_int(
            env.get("CANDIDATE_TICK_STALE_SEC", "90"),
            "CANDIDATE_TICK_STALE_SEC",
            min_value=1,
        ),
        candidate_stale_requires_tick_stale=_parse_bool(
            env.get("CANDIDATE_STALE_REQUIRES_TICK_STALE", "true")
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
        risk_cross_exchange_divergence_bp=_parse_float(
            env.get("RISK_CROSS_EXCHANGE_DIVERGENCE_BP", "0"),
            "RISK_CROSS_EXCHANGE_DIVERGENCE_BP",
            min_value=0.0,
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
        entry_timing_premarket_context_enabled=_parse_bool(
            env.get("ENTRY_TIMING_PREMARKET_CONTEXT_ENABLED", "false")
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
        live_sim_order_exchange=env.get("LIVE_SIM_ORDER_EXCHANGE", "KRX"),
        live_sim_nxt_support_confirmed=_parse_bool(
            env.get("LIVE_SIM_NXT_SUPPORT_CONFIRMED", "false")
        ),
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
        live_sim_max_daily_loss=_parse_float(
            env.get("LIVE_SIM_MAX_DAILY_LOSS", "0"),
            "LIVE_SIM_MAX_DAILY_LOSS",
            min_value=0.0,
        ),
        live_sim_max_daily_loss_pct=_parse_float(
            env.get("LIVE_SIM_MAX_DAILY_LOSS_PCT", "0.0"),
            "LIVE_SIM_MAX_DAILY_LOSS_PCT",
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
        live_sim_preflight_pending_command_backlog_warn_threshold=_parse_int(
            env.get("LIVE_SIM_PREFLIGHT_PENDING_COMMAND_BACKLOG_WARN_THRESHOLD", "30"),
            "LIVE_SIM_PREFLIGHT_PENDING_COMMAND_BACKLOG_WARN_THRESHOLD",
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
        live_sim_entry_window_start=env.get(
            "LIVE_SIM_ENTRY_WINDOW_START",
            "09:05:00",
        ),
        live_sim_entry_window_end=env.get(
            "LIVE_SIM_ENTRY_WINDOW_END",
            "14:30:00",
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
        live_sim_buy_price_offset_ticks=_parse_int(
            env.get("LIVE_SIM_BUY_PRICE_OFFSET_TICKS", "1"),
            "LIVE_SIM_BUY_PRICE_OFFSET_TICKS",
            min_value=0,
        ),
        live_sim_reprice_enabled=_parse_bool(
            env.get("LIVE_SIM_REPRICE_ENABLED", "false")
        ),
        live_sim_reprice_max_attempts=_parse_int(
            env.get("LIVE_SIM_REPRICE_MAX_ATTEMPTS", "1"),
            "LIVE_SIM_REPRICE_MAX_ATTEMPTS",
            min_value=1,
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
        live_sim_reconcile_notional_tolerance=_parse_float(
            env.get("LIVE_SIM_RECONCILE_NOTIONAL_TOLERANCE", "1.0"),
            "LIVE_SIM_RECONCILE_NOTIONAL_TOLERANCE",
            min_value=0.0,
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
        live_sim_operating_loop_enabled=_parse_bool(
            env.get("LIVE_SIM_OPERATING_LOOP_ENABLED", "false")
        ),
        live_sim_operating_loop_queue_commands=_parse_bool(
            env.get("LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS", "false")
        ),
        live_sim_operating_loop_interval_sec=_parse_int(
            env.get("LIVE_SIM_OPERATING_LOOP_INTERVAL_SEC", "20"),
            "LIVE_SIM_OPERATING_LOOP_INTERVAL_SEC",
            min_value=5,
        ),
        live_sim_operating_loop_market_open_time=env.get(
            "LIVE_SIM_OPERATING_LOOP_MARKET_OPEN_TIME",
            "09:05:00",
        ),
        live_sim_operating_loop_market_close_time=env.get(
            "LIVE_SIM_OPERATING_LOOP_MARKET_CLOSE_TIME",
            "15:20:00",
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
        dashboard_snapshot_sections_enabled=_parse_bool(
            env.get("DASHBOARD_SNAPSHOT_SECTIONS_ENABLED", "true")
        ),
        dashboard_snapshot_fast_cache_ttl_sec=_parse_float(
            env.get("DASHBOARD_SNAPSHOT_FAST_CACHE_TTL_SEC", "2.0"),
            "DASHBOARD_SNAPSHOT_FAST_CACHE_TTL_SEC",
            min_value=0.0,
        ),
        dashboard_snapshot_fast_default_limit=_parse_int(
            env.get("DASHBOARD_SNAPSHOT_FAST_DEFAULT_LIMIT", "20"),
            "DASHBOARD_SNAPSHOT_FAST_DEFAULT_LIMIT",
            min_value=1,
        ),
        dashboard_snapshot_fast_timeout_budget_ms=_parse_int(
            env.get("DASHBOARD_SNAPSHOT_FAST_TIMEOUT_BUDGET_MS", "5000"),
            "DASHBOARD_SNAPSHOT_FAST_TIMEOUT_BUDGET_MS",
            min_value=100,
        ),
        dashboard_snapshot_warn_latency_ms=_parse_int(
            env.get("DASHBOARD_SNAPSHOT_WARN_LATENCY_MS", "3000"),
            "DASHBOARD_SNAPSHOT_WARN_LATENCY_MS",
            min_value=1,
        ),
        dashboard_snapshot_fail_latency_ms=_parse_int(
            env.get("DASHBOARD_SNAPSHOT_FAIL_LATENCY_MS", "10000"),
            "DASHBOARD_SNAPSHOT_FAIL_LATENCY_MS",
            min_value=1,
        ),
        deprecated_flag_warnings=_deprecated_flag_warnings(env),
    )


_DEPRECATED_FLAG_RULES: dict[str, DeprecatedFlagWarning] = {
    "TRADING_MODE": DeprecatedFlagWarning(
        flag="TRADING_MODE",
        status="LEGACY_PROFILE_SELECTOR",
        replacement="TRADING_PROFILE",
        message=(
            "TRADING_MODE is retained for compatibility; choose OBSERVE or "
            "LIVE_SIM_PILOT through TRADING_PROFILE first."
        ),
    ),
    "TRADING_ALLOW_LIVE_SIM": DeprecatedFlagWarning(
        flag="TRADING_ALLOW_LIVE_SIM",
        status="LEGACY_ENABLE_SWITCH",
        replacement="TRADING_PROFILE=LIVE_SIM_PILOT plus LIVE_SIM safety flags",
        message=(
            "TRADING_ALLOW_LIVE_SIM is a legacy safety switch and no longer the "
            "capability source of truth."
        ),
    ),
    "STRATEGY_ENGINE_OBSERVE_ONLY": DeprecatedFlagWarning(
        flag="STRATEGY_ENGINE_OBSERVE_ONLY",
        status="LEGACY_OBSERVE_FLAG",
        replacement="TRADING_PROFILE capabilities and admission_trace",
        message=(
            "Strategy observation remains read-only; order admission is decided "
            "outside this flag."
        ),
    ),
    "RISK_GATE_OBSERVE_ONLY": DeprecatedFlagWarning(
        flag="RISK_GATE_OBSERVE_ONLY",
        status="LEGACY_OBSERVE_FLAG",
        replacement="TRADING_PROFILE capabilities and admission_trace",
        message=(
            "Risk observation remains read-only; OBSERVE_PASS is not an order "
            "approval flag."
        ),
    ),
}


def _deprecated_flag_warnings(env: Mapping[str, str]) -> tuple[DeprecatedFlagWarning, ...]:
    return tuple(
        warning for flag, warning in _DEPRECATED_FLAG_RULES.items() if flag in env
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


def _normalize_live_sim_order_exchange(value: object) -> str:
    text = str(value or "KRX").strip().upper()
    aliases = {
        "": "KRX",
        "K": "KRX",
        "KRX": "KRX",
        "N": "NXT",
        "NX": "NXT",
        "NXT": "NXT",
        "S": "SOR",
        "SO": "SOR",
        "SOR": "SOR",
        "A": "SOR",
        "AL": "SOR",
        "ALL": "SOR",
        "INTEGRATED": "SOR",
    }
    exchange = aliases.get(text)
    if exchange not in {"KRX", "NXT", "SOR"}:
        raise ValueError("LIVE_SIM_ORDER_EXCHANGE must be one of KRX, NXT, SOR")
    return exchange


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


def _parse_key_value_mapping(value: str, field_name: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in value.split(","):
        normalized_part = part.strip()
        if not normalized_part:
            raise ValueError(f"{field_name} must be a comma-separated key=value map")
        if "=" not in normalized_part:
            raise ValueError(f"{field_name} must be a comma-separated key=value map")
        key, raw_value = normalized_part.split("=", 1)
        normalized_key = _normalize_non_empty(key)
        normalized_value = _require_non_empty_config(raw_value).upper()
        if normalized_key in result:
            raise ValueError(f"{field_name} must not contain duplicate keys")
        result[normalized_key] = normalized_value
    return result


def _normalize_required_mapping(
    value: Mapping[str, str],
    field_name: str,
    *,
    required_keys: Iterable[str],
) -> dict[str, str]:
    normalized = {
        _normalize_non_empty(key): _require_non_empty_config(raw_value).upper()
        for key, raw_value in dict(value).items()
    }
    missing = [key for key in required_keys if _normalize_non_empty(key) not in normalized]
    if missing:
        raise ValueError(f"{field_name} missing required keys: {','.join(missing)}")
    return normalized


def _normalize_market_scan_markets(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = _normalize_list_values(values)
    allowed = {"KOSPI", "KOSDAQ"}
    unsupported = [value for value in normalized if value not in allowed]
    if unsupported:
        raise ValueError("MARKET_SCAN_MARKETS must contain only KOSPI,KOSDAQ")
    return normalized


def _normalize_stock_code_list(values: tuple[str, ...]) -> tuple[str, ...]:
    from domain.broker.utils import validate_stock_code

    normalized = tuple(validate_stock_code(value) for value in values)
    if len(set(normalized)) != len(normalized):
        raise ValueError("configuration stock code list values must not contain duplicates")
    return normalized


def _parse_stock_code_csv_list(value: str, field_name: str) -> tuple[str, ...]:
    parts = tuple(part.strip() for part in value.split(","))
    if any(part == "" for part in parts):
        raise ValueError(f"{field_name} must be a comma-separated non-empty stock code list")
    try:
        return _normalize_stock_code_list(parts)
    except ValueError as exc:
        raise ValueError(f"{field_name} must contain 6-digit domestic stock codes") from exc


def _parse_intervals(value: str) -> tuple[int, ...]:
    parts = [part.strip() for part in value.split(",")]
    if any(part == "" for part in parts):
        raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC must be a comma-separated integer list")
    try:
        intervals = [int(part) for part in parts]
    except ValueError as exc:
        raise ValueError("MARKET_DATA_BAR_INTERVALS_SEC must contain integers") from exc
    return normalize_interval_list(intervals)
