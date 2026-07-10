from pathlib import Path

from gateway.settings import load_gateway_settings
from services.config import TradingMode, TradingProfile, clear_settings_cache, load_settings


def test_default_settings_are_observe_with_live_flags_disabled() -> None:
    settings = load_settings({})
    gateway_settings = load_gateway_settings({})

    assert settings.trading_profile is TradingProfile.OBSERVE
    assert settings.trading_mode is TradingMode.OBSERVE
    assert settings.live_sim_allowed is False
    assert settings.live_real_allowed is False
    assert settings.trading_capabilities.profile is TradingProfile.OBSERVE
    assert settings.trading_capabilities.observation_allowed is True
    assert settings.trading_capabilities.dry_run_shadow_allowed is False
    assert settings.trading_capabilities.live_sim_intent_allowed is False
    assert settings.trading_capabilities.live_sim_order_plan_allowed is False
    assert settings.trading_capabilities.live_sim_gateway_command_allowed is False
    assert settings.trading_capabilities.live_real_order_allowed is False
    assert settings.trading_capabilities.broker_order_path == "OBSERVE_ONLY"
    assert settings.deprecated_flag_warnings == ()
    assert settings.ai_sidecar_enabled is False
    assert settings.ai_sidecar_intraday_allowed is False
    assert settings.ai_sidecar_order_context_allowed is False
    assert settings.ai_sidecar_model == ""
    assert settings.ai_sidecar_openai_api_key_env == "OPENAI_API_KEY"
    assert settings.ai_sidecar_openai_base_url == ""
    assert settings.ai_sidecar_use_responses_api is True
    assert settings.ai_sidecar_structured_outputs_enabled is True
    assert settings.ai_sidecar_strict_schema is True
    assert settings.ai_sidecar_tools_enabled is False
    assert settings.ai_sidecar_order_tools_enabled is False
    assert settings.ai_sidecar_max_output_chars == 6000
    assert settings.ai_sidecar_max_retries == 1
    assert settings.ai_sidecar_store_raw_response is False
    assert settings.ai_sidecar_allow_manual_run is True
    assert settings.ai_sidecar_request_retention_days == 30
    assert settings.ai_sidecar_default_operator_action == "REVIEW_ONLY"
    assert settings.ai_sidecar_context_builder_enabled is True
    assert settings.ai_sidecar_context_default_limit == 50
    assert settings.ai_sidecar_context_max_limit == 200
    assert settings.ai_sidecar_context_persist_preview is False
    assert settings.ai_sidecar_context_schema_version == "ai-sidecar-context.v1"
    assert settings.ai_sidecar_context_redact_paths is True
    assert settings.ai_sidecar_context_redact_secrets is True
    assert settings.ai_sidecar_context_include_raw_payload is False
    assert settings.ai_candidate_scorer_enabled is False
    assert settings.ai_candidate_scorer_provider == "mock"
    assert settings.ai_external_llm_enabled is False
    assert settings.ai_external_llm_provider == "none"
    assert settings.ai_external_llm_model == ""
    assert settings.ai_external_llm_api_key_env == "OPENAI_API_KEY"
    assert settings.ai_external_llm_base_url == ""
    assert settings.ai_external_llm_timeout_seconds == 10
    assert settings.ai_external_llm_max_retries == 1
    assert settings.ai_external_llm_retry_backoff_seconds == 0.5
    assert settings.ai_external_llm_max_response_chars == 8000
    assert settings.ai_external_llm_temperature == 0
    assert settings.ai_external_llm_require_json_schema is True
    assert settings.ai_external_llm_store_request is False
    assert settings.ai_external_llm_store_response is False
    assert settings.ai_external_llm_redact_prompt is True
    assert settings.ai_external_llm_fail_open is True
    assert settings.ai_external_llm_daily_call_limit == 100
    assert settings.ai_external_llm_per_run_call_limit == 1
    assert settings.ai_external_llm_cost_guard_enabled is True
    assert settings.ai_external_llm_allow_network is False
    assert settings.market_data_enabled is True
    assert settings.market_data_tick_stale_sec == 10
    assert settings.market_data_degraded_tick_stale_sec == 30
    assert settings.market_data_bar_intervals_sec == (60, 180, 300)
    assert settings.market_data_premarket_snapshot_enabled is False
    assert settings.market_data_projection_reconcile_limit == 500
    assert settings.market_data_reconcile_live_default_persist is False
    assert settings.market_data_reconcile_locked_fallback_to_read_only is True
    assert settings.operator_sqlite_lock_retry_attempts == 3
    assert settings.operator_sqlite_lock_retry_base_sleep_sec == 0.05
    assert settings.operator_sqlite_lock_retry_max_sleep_sec == 0.5
    assert settings.operator_sqlite_busy_timeout_ms == 500
    assert settings.operator_run_once_locked_http_status == 409
    assert settings.ops_script_locked_retry_attempts == 3
    assert settings.ops_script_locked_retry_sleep_sec == 1.0
    assert settings.gateway_market_data_append_only_dry_run_enabled is False
    assert settings.gateway_market_data_append_only_cutover_enabled is False
    assert settings.gateway_market_data_append_only_operating_mode == "OFF"
    assert settings.gateway_market_data_append_only_global_kill_switch is True
    assert settings.gateway_market_data_append_only_auto_rollback_enabled is True
    assert settings.gateway_market_data_append_only_global_max_skip_per_minute == 0
    assert settings.gateway_market_data_append_only_max_error_count == 0
    assert settings.gateway_market_data_append_only_max_dead_letter_count == 0
    assert settings.gateway_market_data_append_only_max_pending_within_sla == 100
    assert (
        settings.gateway_market_data_append_only_max_condition_event_pending_within_sla
        == 10
    )
    assert settings.gateway_market_data_append_only_require_dashboard_fast_ok is True
    assert settings.gateway_market_data_append_only_require_backlog_ready is True
    assert settings.gateway_market_data_append_only_auto_rollback_cooldown_sec == 300
    assert settings.gateway_market_data_append_only_health_stale_sec == 60
    assert settings.gateway_market_data_append_only_price_tick_cutover_enabled is False
    assert settings.gateway_market_data_append_only_tr_response_dry_run_enabled is False
    assert settings.gateway_market_data_append_only_tr_response_cutover_enabled is False
    assert (
        settings.gateway_market_data_append_only_tr_response_require_worker_side_effects
        is True
    )
    assert (
        settings.gateway_market_data_append_only_condition_event_dry_run_enabled
        is False
    )
    assert (
        settings.gateway_market_data_append_only_condition_event_cutover_enabled
        is False
    )
    assert (
        settings.gateway_market_data_append_only_condition_event_require_worker_side_effects
        is True
    )
    assert (
        settings.gateway_market_data_append_only_condition_event_require_fusion_enabled
        is True
    )
    assert (
        settings.gateway_market_data_append_only_condition_event_require_backlog_ready
        is True
    )
    assert settings.gateway_market_data_append_only_condition_event_max_skip_per_minute == 0
    assert (
        settings.gateway_market_data_append_only_condition_event_fail_closed_on_side_effect_error
        is True
    )
    assert (
        settings.gateway_market_data_append_only_condition_event_allow_candidate_ingest_in_worker
        is False
    )
    assert settings.gateway_market_data_append_only_condition_event_max_payload_age_sec == 60
    assert settings.gateway_market_data_append_only_cutover_event_types == ("price_tick",)
    assert settings.gateway_market_data_append_only_require_reconcile_pass is True
    assert settings.gateway_market_data_append_only_require_latest_reconcile_pass is True
    assert settings.gateway_market_data_append_only_require_worker_apply_enabled is True
    assert settings.gateway_market_data_append_only_fail_closed_on_routing_error is True
    assert settings.gateway_market_data_append_only_price_tick_max_skip_per_minute == 0
    assert settings.gateway_market_data_append_only_reconcile_max_age_sec == 300
    assert settings.gateway_market_data_append_only_event_types == (
        "price_tick",
        "condition_event",
        "tr_response",
    )
    assert settings.gateway_market_data_append_only_min_outbox_status == "ENQUEUED"
    assert settings.gateway_market_reference_append_only_dry_run_enabled is False
    assert settings.gateway_market_reference_append_only_cutover_enabled is False
    assert settings.gateway_market_reference_append_only_global_kill_switch is True
    assert settings.gateway_market_reference_append_only_max_skip_per_minute == 0
    assert settings.gateway_market_reference_append_only_max_pending_within_sla == 1
    assert settings.gateway_market_reference_append_only_require_reconcile_pass is True
    assert settings.gateway_market_reference_append_only_reconcile_max_age_sec == 300
    assert settings.gateway_market_reference_append_only_min_membership_count == 100
    assert (
        settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
        is True
    )
    assert settings.gateway_market_index_append_only_dry_run_enabled is False
    assert settings.gateway_market_index_append_only_cutover_enabled is False
    assert settings.gateway_market_index_append_only_global_kill_switch is True
    assert settings.gateway_market_index_append_only_max_skip_per_minute == 0
    assert settings.gateway_market_index_append_only_max_pending_within_sla == 1
    assert settings.gateway_market_index_append_only_require_reconcile_pass is True
    assert settings.gateway_market_index_append_only_require_data_usable is True
    assert settings.gateway_market_index_append_only_require_parser_verified is True
    assert settings.gateway_market_index_append_only_require_worker_regime_refresh is True
    assert (
        settings.gateway_market_index_append_only_fail_closed_on_regime_refresh_error
        is True
    )
    assert settings.gateway_market_index_append_only_reconcile_max_age_sec == 300
    assert settings.gateway_market_index_append_only_max_event_age_sec == 30
    assert settings.gateway_market_index_append_only_max_future_skew_sec == 5
    assert settings.gateway_market_index_append_only_require_fresh_gateway_health is True
    assert settings.gateway_market_index_append_only_gateway_health_max_age_sec == 30
    assert settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15 is True
    assert settings.gateway_market_regime_append_only_dry_run_enabled is False
    assert settings.gateway_market_regime_append_only_cutover_enabled is False
    assert settings.gateway_market_regime_append_only_global_kill_switch is True
    assert settings.gateway_market_regime_append_only_max_skip_per_minute == 0
    assert settings.gateway_market_regime_append_only_max_pending_within_sla == 1
    assert settings.gateway_market_regime_append_only_require_reconcile_pass is True
    assert settings.gateway_market_regime_append_only_require_prior_event_reconcile is True
    assert settings.gateway_market_regime_append_only_require_index_routing_guard is True
    assert settings.gateway_market_regime_append_only_require_worker_context_refresh is True
    assert (
        settings.gateway_market_regime_append_only_fail_closed_on_context_refresh_error
        is True
    )
    assert settings.gateway_market_regime_append_only_reconcile_max_age_sec == 300
    assert settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18 is True
    assert settings.gateway_market_scan_append_only_dry_run_enabled is False
    assert settings.gateway_market_scan_append_only_cutover_enabled is False
    assert settings.gateway_market_scan_append_only_global_kill_switch is True
    assert settings.gateway_market_scan_append_only_max_skip_per_minute == 0
    assert settings.gateway_market_scan_append_only_require_reconcile_pass is True
    assert settings.gateway_market_scan_append_only_require_prior_event_reconcile is True
    assert settings.gateway_market_scan_append_only_require_parser_verified is True
    assert settings.gateway_market_scan_append_only_require_market_data_dependency is True
    assert settings.gateway_market_scan_append_only_require_worker_closure is True
    assert settings.gateway_market_scan_append_only_fail_closed_on_worker_error is True
    assert settings.gateway_market_scan_append_only_max_pending_within_sla == 4
    assert settings.gateway_market_scan_append_only_reconcile_max_age_sec == 300
    assert settings.gateway_market_scan_append_only_max_event_age_sec == 120
    assert settings.gateway_market_scan_append_only_max_future_skew_sec == 5
    assert settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20 is True
    assert settings.market_context_snapshot_stale_sec == 30
    assert settings.event_store_retention_enabled is False
    assert settings.projection_event_result_backfill_enabled is False
    assert settings.event_store_retention_days == 30
    assert settings.event_store_retention_batch_size == 5000
    assert settings.event_store_retention_interval_sec == 86400
    assert gateway_settings.kiwoom_market_index_enabled is False
    assert gateway_settings.kiwoom_market_index_realtime_enabled is False
    assert gateway_settings.kiwoom_market_index_tr_bootstrap_enabled is False
    assert gateway_settings.kiwoom_market_index_codes == ("KOSPI", "KOSDAQ")
    assert gateway_settings.kiwoom_market_index_screen_no == "5700"
    assert gateway_settings.kiwoom_market_index_poll_sec == 60.0
    assert settings.realtime_subscription_enabled is True
    assert settings.realtime_subscription_queue_commands is False
    assert settings.realtime_subscription_max_total == 50
    assert settings.realtime_subscription_max_per_theme == 5
    assert settings.realtime_subscription_anchor_codes == ("005930", "000660")
    assert settings.realtime_subscription_stale_sec == 60
    assert settings.realtime_subscription_remove_stale_after_sec == 600
    assert settings.realtime_subscription_allow_remove is False
    assert settings.realtime_subscription_exchange == "KRX"
    assert settings.theme_service_enabled is True
    assert settings.theme_min_active_members == 2
    assert settings.theme_min_fresh_coverage_ratio == 0.3
    assert settings.theme_observable_coverage_enabled is True
    assert settings.theme_min_observable_members == 3
    assert settings.theme_leading_rising_ratio == 0.5
    assert settings.theme_spreading_rising_ratio == 0.35
    assert settings.theme_import_allow_replace is False
    assert settings.theme_premarket_observables_enabled is False
    assert settings.naver_theme_import_enabled is False
    assert settings.naver_theme_import_base_url == "https://finance.naver.com/sise/theme.naver"
    assert settings.naver_theme_import_timeout_seconds == 10
    assert settings.naver_theme_import_max_themes == 500
    assert settings.naver_theme_import_request_sleep_seconds == 0.3
    assert settings.naver_theme_import_replace is False
    assert settings.naver_theme_import_min_member_count == 2
    assert settings.naver_theme_import_abort_on_empty is True
    assert settings.condition_fusion_event_incremental_enabled is True
    assert settings.condition_fusion_sweep_enabled is True
    assert settings.condition_fusion_sweep_interval_sec == 60
    assert settings.incremental_evaluation_enabled is True
    assert settings.incremental_evaluation_worker_enabled is True
    assert settings.incremental_evaluation_worker_interval_sec == 1.0
    assert settings.incremental_evaluation_batch_size == 20
    assert settings.incremental_evaluation_retry_limit == 3
    assert settings.projection_outbox_worker_enabled is False
    assert settings.projection_outbox_apply_projection_enabled is False
    assert settings.projection_outbox_market_data_apply_enabled is False
    assert settings.projection_outbox_market_reference_apply_enabled is False
    assert settings.projection_outbox_market_index_apply_enabled is False
    assert settings.projection_outbox_market_regime_apply_enabled is False
    assert settings.projection_outbox_market_scan_apply_enabled is False
    assert settings.projection_outbox_apply_batch_size == 50
    assert settings.projection_outbox_market_reference_apply_batch_size == 20
    assert settings.projection_outbox_market_index_apply_batch_size == 20
    assert settings.projection_outbox_market_regime_apply_batch_size == 20
    assert settings.projection_outbox_market_scan_apply_batch_size == 20
    assert settings.projection_outbox_live_run_once_batch_size == 50
    assert settings.projection_outbox_run_once_max_wall_ms == 5000
    assert settings.projection_outbox_apply_min_age_sec == 1.0
    assert settings.projection_outbox_market_reference_apply_min_age_sec == 1.0
    assert settings.projection_outbox_market_index_apply_min_age_sec == 1.0
    assert settings.projection_outbox_market_regime_apply_min_age_sec == 1.0
    assert settings.projection_outbox_market_scan_apply_min_age_sec == 1.0
    assert settings.live_sim_lifecycle_consumer_enabled is False
    assert settings.live_sim_lifecycle_worker_enabled is False
    assert settings.live_sim_lifecycle_worker_interval_sec == 1.0
    assert settings.live_sim_lifecycle_batch_size == 20
    assert settings.live_sim_lifecycle_retry_limit == 3
    assert settings.live_sim_lifecycle_processing_ttl_sec == 60
    assert settings.live_sim_lifecycle_retry_delay_sec == 1.0
    assert settings.live_sim_lifecycle_cutover_dry_run_enabled is False
    assert settings.live_sim_lifecycle_cutover_enabled is False
    assert settings.live_sim_lifecycle_global_kill_switch is True
    assert settings.live_sim_lifecycle_inline_fallback_enabled is True
    assert settings.live_sim_lifecycle_require_worker_health is True
    assert settings.live_sim_lifecycle_worker_health_max_age_sec == 10
    assert settings.live_sim_lifecycle_max_unresolved_count == 100
    assert settings.candidate_fsm_enabled is True
    assert settings.candidate_trade_date_timezone == "Asia/Seoul"
    assert settings.candidate_source_stale_sec == 300
    assert settings.candidate_tick_stale_sec == 90
    assert settings.candidate_stale_requires_tick_stale is True
    assert settings.candidate_episode_ttl_sec == 1800
    assert settings.candidate_context_require_1m_bar is True
    assert settings.candidate_context_require_vwap is False
    assert settings.candidate_max_active_per_code == 1
    assert settings.candidate_theme_source_states == ("LEADING", "SPREADING")
    assert settings.candidate_theme_member_roles == (
        "LEADER_CANDIDATE",
        "CO_LEADER_CANDIDATE",
        "FOLLOWER_CANDIDATE",
    )
    assert settings.strategy_engine_enabled is True
    assert settings.strategy_engine_observe_only is True
    assert settings.strategy_engine_max_candidates == 500
    assert settings.strategy_engine_require_context_ready is True
    assert settings.strategy_engine_allowed_candidate_states == ("CONTEXT_READY", "WATCHING")
    assert settings.strategy_engine_stale_tick_sec == 30
    assert settings.strategy_engine_allowed_theme_states == ("LEADING", "SPREADING")
    assert settings.strategy_engine_allowed_theme_roles == (
        "LEADER_CANDIDATE",
        "CO_LEADER_CANDIDATE",
        "FOLLOWER_CANDIDATE",
    )
    assert settings.strategy_engine_require_1m_bar is True
    assert settings.strategy_engine_require_vwap is False
    assert settings.strategy_pullback_min_pct == 0.3
    assert settings.strategy_pullback_max_pct == 5.0
    assert settings.strategy_vwap_reclaim_tolerance_pct == 1.0
    assert settings.strategy_min_trade_value_delta_1m == 0
    assert settings.strategy_min_trade_value_delta_3m == 0
    assert settings.strategy_breakout_retest_near_high_pct == 2.0
    assert settings.strategy_follower_expansion_min_theme_rising_ratio == 0.35
    assert settings.strategy_config_version == "observe_v1"
    assert settings.risk_gate_enabled is True
    assert settings.risk_gate_observe_only is True
    assert settings.risk_gate_max_strategy_observations == 500
    assert settings.risk_gate_require_strategy_matched is True
    assert settings.risk_gate_stale_tick_sec == 30
    assert settings.risk_gate_strategy_stale_sec == 300
    assert settings.risk_gate_max_spread_ticks == 5
    assert settings.risk_gate_min_trade_value_delta_1m == 0
    assert settings.risk_gate_min_cumulative_trade_value == 0
    assert settings.risk_gate_min_execution_strength == 0
    assert settings.risk_gate_max_change_rate == 25.0
    assert settings.risk_gate_max_vwap_extension_pct == 8.0
    assert settings.risk_gate_near_day_high_pct == 1.0
    assert settings.risk_gate_min_theme_fresh_coverage_ratio == 0.3
    assert settings.risk_gate_min_theme_rising_ratio == 0.35
    assert settings.risk_cross_exchange_divergence_bp == 0
    assert settings.risk_gate_duplicate_active_candidate_limit == 1
    assert settings.risk_gate_observation_cooldown_sec == 60
    assert settings.risk_gate_config_version == "observe_v1"
    assert settings.entry_timing_enabled is True
    assert settings.entry_timing_write_order_plan_drafts is True
    assert settings.entry_timing_max_plans_per_run == 20
    assert settings.entry_timing_plan_ttl_seconds == 90
    assert settings.entry_timing_pullback_min_pct == 1.0
    assert settings.entry_timing_pullback_max_pct == 4.5
    assert settings.entry_timing_vwap_reclaim_tolerance_pct == 0.7
    assert settings.entry_timing_vwap_overextended_pct == 3.0
    assert settings.entry_timing_chase_near_high_pct == 0.7
    assert settings.entry_timing_max_spread_ticks == 3
    assert settings.entry_timing_min_turnover_krw == 500_000_000
    assert settings.entry_timing_min_execution_strength == 100
    assert settings.entry_timing_default_notional == 100_000
    assert settings.entry_timing_max_notional == 100_000
    assert settings.entry_timing_allow_market_order is False
    assert settings.entry_timing_price_offset_ticks == 0
    assert settings.entry_timing_allow_follower_in_spreading is True
    assert settings.entry_timing_allow_follower_in_leader_only is False
    assert settings.entry_timing_require_risk_observe_pass is False
    assert settings.entry_timing_require_strategy_matched is False
    assert settings.entry_timing_stale_max_seconds == 60
    assert settings.entry_timing_premarket_context_enabled is False
    assert settings.entry_timing_config_version == "entry_timing_v1"
    assert settings.dry_run_oms_enabled is False
    assert settings.dry_run_intent_creation_enabled is False
    assert settings.dry_run_simulated_fill_enabled is False
    assert settings.dry_run_require_safety_gate is True
    assert settings.dry_run_require_strategy_matched is True
    assert settings.dry_run_require_risk_observe_pass is True
    assert settings.dry_run_require_candidate_context_ready is True
    assert settings.dry_run_max_daily_intents == 20
    assert settings.dry_run_max_active_positions == 5
    assert settings.dry_run_max_position_notional == 1_000_000
    assert settings.dry_run_default_position_notional == 1_000_000
    assert settings.dry_run_min_quantity == 1
    assert settings.dry_run_intent_ttl_sec == 300
    assert settings.dry_run_duplicate_cooldown_sec == 300
    assert settings.dry_run_stale_tick_sec == 30
    assert settings.dry_run_commission_rate == 0
    assert settings.dry_run_tax_rate == 0
    assert settings.dry_run_allow_sell is False
    assert settings.dry_run_allow_short is False
    assert settings.dry_run_allow_market_sim is True
    assert settings.dry_run_order_routing_enabled is False
    assert settings.dry_run_gateway_command_enabled is False
    assert settings.dry_run_exit_engine_enabled is False
    assert settings.dry_run_exit_intent_creation_enabled is False
    assert settings.dry_run_exit_order_creation_enabled is False
    assert settings.dry_run_exit_simulated_fill_enabled is False
    assert settings.dry_run_exit_require_safety_gate is True
    assert settings.dry_run_exit_stop_loss_pct == 2.0
    assert settings.dry_run_exit_take_profit_pct == 5.0
    assert settings.dry_run_exit_trailing_stop_pct == 3.0
    assert settings.dry_run_exit_max_hold_sec == 1800
    assert settings.dry_run_exit_stale_tick_sec == 30
    assert settings.dry_run_exit_min_hold_sec == 0
    assert settings.dry_run_exit_intent_ttl_sec == 300
    assert settings.dry_run_exit_allow_sell_close_only is True
    assert settings.dry_run_exit_allow_short is False
    assert settings.dry_run_exit_order_routing_enabled is False
    assert settings.dry_run_exit_gateway_command_enabled is False
    assert settings.dry_run_exit_config_version == "exit_dry_run_v1"
    assert settings.live_sim_max_daily_loss == 0.0
    assert settings.live_sim_max_daily_loss_pct == 0.0
    assert settings.live_sim_order_exchange == "KRX"
    assert settings.live_sim_nxt_support_confirmed is False
    assert settings.live_sim_pilot_pipeline_enabled is False
    assert settings.live_sim_pilot_auto_queue_command is False
    assert settings.live_sim_order_plan_routing_enabled is False
    assert settings.live_sim_order_plan_require_plan_ready is True
    assert settings.live_sim_order_plan_require_fresh_tick is True
    assert settings.live_sim_order_plan_stale_sec == 30
    assert settings.live_sim_order_plan_max_price_drift_pct == 0.8
    assert settings.live_sim_order_plan_require_strategy_matched is True
    assert settings.live_sim_order_plan_require_risk_observe_pass is True
    assert settings.live_sim_order_plan_require_candidate_context_ready is True
    assert settings.live_sim_order_plan_require_dry_run_evidence is False
    assert settings.live_sim_order_plan_max_plans_per_run == 3
    assert settings.live_sim_order_plan_max_commands_per_run == 1
    assert settings.live_sim_order_plan_min_notional == 10_000
    assert settings.live_sim_order_plan_default_notional == 100_000
    assert settings.live_sim_order_plan_max_notional == 100_000
    assert settings.live_sim_order_plan_allow_market_order is False
    assert settings.live_sim_order_plan_allowed_side == "BUY"
    assert settings.live_sim_price_offset_ticks == 0
    assert settings.live_sim_buy_price_offset_ticks == 1
    assert settings.live_sim_reprice_enabled is False
    assert settings.live_sim_reprice_max_attempts == 1
    assert settings.live_sim_preflight_pending_command_backlog_warn_threshold == 30
    assert settings.live_sim_entry_window_start == "09:05:00"
    assert settings.live_sim_entry_window_end == "14:30:00"
    assert settings.live_sim_exit_eod_flatten_time == "15:15:00"
    assert settings.live_sim_reconcile_notional_tolerance == 1.0
    assert settings.live_sim_operating_loop_enabled is False
    assert settings.live_sim_operating_loop_queue_commands is False
    assert settings.live_sim_operating_loop_interval_sec == 20
    assert settings.live_sim_operating_loop_market_open_time == "09:05:00"
    assert settings.live_sim_operating_loop_market_close_time == "15:20:00"
    assert settings.market_scan_enabled is False
    assert settings.market_scan_interval_sec == 120
    assert settings.market_scan_top_n == 200
    assert settings.market_scan_tr_codes == {
        "TRADE_VALUE": "OPT10032",
        "CHANGE_RATE": "OPT10027",
    }
    assert settings.market_scan_markets == ("KOSPI", "KOSDAQ")
    assert settings.market_scan_market_codes == {"KOSPI": "001", "KOSDAQ": "101"}
    assert settings.market_scan_parser_status == "PILOT_UNVERIFIED"
    assert settings.theme_snapshot_stale_sec == 300
    assert settings.dashboard_enabled is True
    assert settings.dashboard_refresh_sec == 5
    assert settings.dashboard_snapshot_default_limit == 50
    assert settings.dashboard_max_limit == 200
    assert settings.dashboard_show_raw_json is True
    assert settings.dashboard_route_enabled is True
    assert settings.dashboard_snapshot_sections_enabled is True
    assert settings.dashboard_snapshot_fast_cache_ttl_sec == 2.0
    assert settings.dashboard_snapshot_fast_default_limit == 20
    assert settings.dashboard_snapshot_fast_timeout_budget_ms == 5000
    assert settings.dashboard_snapshot_warn_latency_ms == 3000
    assert settings.dashboard_snapshot_fail_latency_ms == 10000


def test_projection_outbox_market_data_apply_settings_parse_explicit_flags() -> None:
    settings = load_settings(
        {
            "PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED": "true",
            "PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED": "true",
            "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED": "true",
            "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED": "true",
            "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED": "true",
            "PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED": "true",
            "PROJECTION_OUTBOX_APPLY_BATCH_SIZE": "7",
            "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_BATCH_SIZE": "5",
            "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_BATCH_SIZE": "4",
            "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_BATCH_SIZE": "3",
            "PROJECTION_OUTBOX_MARKET_SCAN_APPLY_BATCH_SIZE": "2",
            "PROJECTION_OUTBOX_LIVE_RUN_ONCE_BATCH_SIZE": "9",
            "PROJECTION_OUTBOX_RUN_ONCE_MAX_WALL_MS": "1500",
            "PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC": "0.25",
            "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_MIN_AGE_SEC": "0.5",
            "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_MIN_AGE_SEC": "0.75",
            "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC": "1.25",
            "PROJECTION_OUTBOX_MARKET_SCAN_APPLY_MIN_AGE_SEC": "1.5",
        }
    )

    assert settings.projection_outbox_apply_projection_enabled is True
    assert settings.projection_outbox_market_data_apply_enabled is True
    assert settings.projection_outbox_market_reference_apply_enabled is True
    assert settings.projection_outbox_market_index_apply_enabled is True
    assert settings.projection_outbox_market_regime_apply_enabled is True
    assert settings.projection_outbox_market_scan_apply_enabled is True
    assert settings.projection_outbox_apply_batch_size == 7
    assert settings.projection_outbox_market_reference_apply_batch_size == 5
    assert settings.projection_outbox_market_index_apply_batch_size == 4
    assert settings.projection_outbox_market_regime_apply_batch_size == 3
    assert settings.projection_outbox_market_scan_apply_batch_size == 2
    assert settings.projection_outbox_live_run_once_batch_size == 9
    assert settings.projection_outbox_run_once_max_wall_ms == 1500
    assert settings.projection_outbox_apply_min_age_sec == 0.25
    assert settings.projection_outbox_market_reference_apply_min_age_sec == 0.5
    assert settings.projection_outbox_market_index_apply_min_age_sec == 0.75
    assert settings.projection_outbox_market_regime_apply_min_age_sec == 1.25
    assert settings.projection_outbox_market_scan_apply_min_age_sec == 1.5


def test_projection_outbox_apply_settings_are_validated() -> None:
    invalid_cases = {
        "PROJECTION_OUTBOX_APPLY_BATCH_SIZE": "0",
        "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_BATCH_SIZE": "0",
        "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_BATCH_SIZE": "0",
        "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_BATCH_SIZE": "0",
        "PROJECTION_OUTBOX_MARKET_SCAN_APPLY_BATCH_SIZE": "0",
        "PROJECTION_OUTBOX_LIVE_RUN_ONCE_BATCH_SIZE": "0",
        "PROJECTION_OUTBOX_RUN_ONCE_MAX_WALL_MS": "0",
        "PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC": "-0.1",
        "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_MIN_AGE_SEC": "-0.1",
        "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_MIN_AGE_SEC": "-0.1",
        "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC": "-0.1",
        "PROJECTION_OUTBOX_MARKET_SCAN_APPLY_MIN_AGE_SEC": "-0.1",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid projection outbox setting: {key}")


def test_market_scan_cutover_settings_parse_and_validate() -> None:
    settings = load_settings(
        {
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED": "true",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED": "true",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH": "false",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "3",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_REQUIRE_PRIOR_EVENT_RECONCILE": "true",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_REQUIRE_WORKER_CLOSURE": "true",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_FAIL_CLOSED_ON_WORKER_ERROR": "true",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_EVENT_AGE_SEC": "90",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_FUTURE_SKEW_SEC": "4",
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR20": "false",
        }
    )

    assert settings.gateway_market_scan_append_only_dry_run_enabled is True
    assert settings.gateway_market_scan_append_only_cutover_enabled is True
    assert settings.gateway_market_scan_append_only_global_kill_switch is False
    assert settings.gateway_market_scan_append_only_max_skip_per_minute == 3
    assert settings.gateway_market_scan_append_only_max_event_age_sec == 90
    assert settings.gateway_market_scan_append_only_max_future_skew_sec == 4
    assert settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20 is False

    for key, value in {
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "-1",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_EVENT_AGE_SEC": "0",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_FUTURE_SKEW_SEC": "-1",
    }.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid market scan setting: {key}")


def test_trading_profile_capability_matrix() -> None:
    observe = load_settings({"TRADING_PROFILE": "OBSERVE"}).trading_capabilities
    live_sim = load_settings({"TRADING_PROFILE": "LIVE_SIM_PILOT"}).trading_capabilities

    assert observe.to_dict() == {
        "profile": "OBSERVE",
        "observation_allowed": True,
        "dry_run_shadow_allowed": False,
        "live_sim_intent_allowed": False,
        "live_sim_order_plan_allowed": False,
        "live_sim_gateway_command_allowed": False,
        "live_real_order_allowed": False,
        "broker_order_path": "OBSERVE_ONLY",
    }
    assert live_sim.to_dict() == {
        "profile": "LIVE_SIM_PILOT",
        "observation_allowed": True,
        "dry_run_shadow_allowed": True,
        "live_sim_intent_allowed": True,
        "live_sim_order_plan_allowed": True,
        "live_sim_gateway_command_allowed": True,
        "live_real_order_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
    }


def test_deprecated_flag_warnings_point_to_trading_profile() -> None:
    settings = load_settings(
        {
            "TRADING_PROFILE": "LIVE_SIM_PILOT",
            "TRADING_MODE": "LIVE_SIM",
            "TRADING_ALLOW_LIVE_SIM": "true",
            "STRATEGY_ENGINE_OBSERVE_ONLY": "true",
            "RISK_GATE_OBSERVE_ONLY": "true",
        }
    )
    warnings = {item.flag: item for item in settings.deprecated_flag_warnings}

    assert set(warnings) == {
        "TRADING_MODE",
        "TRADING_ALLOW_LIVE_SIM",
        "STRATEGY_ENGINE_OBSERVE_ONLY",
        "RISK_GATE_OBSERVE_ONLY",
    }
    assert warnings["TRADING_MODE"].replacement == "TRADING_PROFILE"
    assert warnings["TRADING_ALLOW_LIVE_SIM"].status == "LEGACY_ENABLE_SWITCH"
    assert settings.deprecated_flag_warning_dicts[0]["flag"] == "TRADING_MODE"
    assert settings.trading_capabilities.live_sim_intent_allowed is True
    assert settings.trading_capabilities.live_real_order_allowed is False


def test_default_gateway_settings_are_mock_local_transport() -> None:
    settings = load_gateway_settings({})

    assert settings.core_url == "http://127.0.0.1:8000"
    assert settings.core_token == ""
    assert settings.source == "mock_gateway"
    assert settings.poll_interval_sec == 1.0
    assert settings.heartbeat_interval_sec == 2.0
    assert settings.event_timeout_sec == 5.0
    assert settings.command_wait_sec == 0.0
    assert settings.command_limit == 20
    assert settings.mock_once is False
    assert settings.mock_price_tick_interval_sec == 2.0


def test_default_environment_settings_are_cached_until_cleared(tmp_path, monkeypatch) -> None:
    first_db = tmp_path / "first.sqlite3"
    second_db = tmp_path / "second.sqlite3"

    clear_settings_cache()
    monkeypatch.setenv("TRADING_DB_PATH", str(first_db))
    first = load_settings()

    monkeypatch.setenv("TRADING_DB_PATH", str(second_db))
    cached = load_settings()
    clear_settings_cache()
    refreshed = load_settings()

    assert first is cached
    assert cached.trading_db_path == first_db
    assert refreshed.trading_db_path == second_db


def test_default_settings_overlay_dotenv_over_process_environment(
    tmp_path,
    monkeypatch,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "# intraday operator overrides",
                "LIVE_SIM_MAX_ORDER_NOTIONAL='3000000'",
                'LIVE_SIM_MAX_DAILY_NOTIONAL="5000000"',
                "TRADING_DB_PATH=storage/from-dotenv.sqlite3",
            )
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("TRADING_ENV_FILE", str(env_file))
    monkeypatch.setenv("LIVE_SIM_MAX_ORDER_NOTIONAL", "100000")
    monkeypatch.setenv("LIVE_SIM_MAX_DAILY_NOTIONAL", "300000")
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "from-process.sqlite3"))
    clear_settings_cache()

    settings = load_settings()

    assert settings.live_sim_max_order_notional == 3_000_000
    assert settings.live_sim_max_daily_notional == 5_000_000
    assert settings.trading_db_path == Path("storage/from-dotenv.sqlite3")


def test_default_settings_fall_back_to_process_environment_without_dotenv(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("LIVE_SIM_MAX_ORDER_NOTIONAL", "123456")
    clear_settings_cache()

    settings = load_settings()

    assert settings.live_sim_max_order_notional == 123_456


def test_explicit_environ_load_does_not_read_dotenv(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "LIVE_SIM_MAX_ORDER_NOTIONAL=3000000",
                "LIVE_SIM_MAX_DAILY_NOTIONAL=5000000",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADING_ENV_FILE", str(env_file))

    settings = load_settings(
        {
            "LIVE_SIM_MAX_ORDER_NOTIONAL": "111111",
            "LIVE_SIM_MAX_DAILY_NOTIONAL": "222222",
        }
    )

    assert settings.live_sim_max_order_notional == 111_111
    assert settings.live_sim_max_daily_notional == 222_222


def test_market_data_interval_settings_are_validated() -> None:
    try:
        load_settings({"MARKET_DATA_BAR_INTERVALS_SEC": "60,90"})
    except ValueError as exc:
        assert "minute-aligned" in str(exc)
    else:
        raise AssertionError("expected invalid market data interval configuration")

    settings = load_settings({"MARKET_DATA_PROJECTION_RECONCILE_LIMIT": "25"})
    assert settings.market_data_projection_reconcile_limit == 25
    operator_settings = load_settings(
        {
            "MARKET_DATA_RECONCILE_LIVE_DEFAULT_PERSIST": "true",
            "MARKET_DATA_RECONCILE_LOCKED_FALLBACK_TO_READ_ONLY": "false",
            "OPERATOR_SQLITE_LOCK_RETRY_ATTEMPTS": "5",
            "OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC": "0.1",
            "OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC": "0.9",
            "OPERATOR_SQLITE_BUSY_TIMEOUT_MS": "250",
            "OPERATOR_RUN_ONCE_LOCKED_HTTP_STATUS": "200",
            "OPS_SCRIPT_LOCKED_RETRY_ATTEMPTS": "4",
            "OPS_SCRIPT_LOCKED_RETRY_SLEEP_SEC": "0.2",
        }
    )
    assert operator_settings.market_data_reconcile_live_default_persist is True
    assert operator_settings.market_data_reconcile_locked_fallback_to_read_only is False
    assert operator_settings.operator_sqlite_lock_retry_attempts == 5
    assert operator_settings.operator_sqlite_lock_retry_base_sleep_sec == 0.1
    assert operator_settings.operator_sqlite_lock_retry_max_sleep_sec == 0.9
    assert operator_settings.operator_sqlite_busy_timeout_ms == 250
    assert operator_settings.operator_run_once_locked_http_status == 200
    assert operator_settings.ops_script_locked_retry_attempts == 4
    assert operator_settings.ops_script_locked_retry_sleep_sec == 0.2

    routing_settings = load_settings(
        {
            "GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED": "true",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED": "true",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE": "market_data_limited",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_ENABLED": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE": "7",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_ERROR_COUNT": "1",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_DEAD_LETTER_COUNT": "2",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "80",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_CONDITION_EVENT_PENDING_WITHIN_SLA": (
                "4"
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_DASHBOARD_FAST_OK": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_BACKLOG_READY": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_COOLDOWN_SEC": "120",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_HEALTH_STALE_SEC": "30",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED": "true",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_DRY_RUN_ENABLED": "true",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED": "true",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_REQUIRE_WORKER_SIDE_EFFECTS": (
                "false"
            ),
            "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_EVENT_TYPES": "price_tick",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_RECONCILE_PASS": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_LATEST_RECONCILE_PASS": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_WORKER_APPLY_ENABLED": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_FAIL_CLOSED_ON_ROUTING_ERROR": "false",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE": "3",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "60",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_EVENT_TYPES": "price_tick,tr_response",
            "GATEWAY_MARKET_DATA_APPEND_ONLY_MIN_OUTBOX_STATUS": "enqueued",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED": "true",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED": "true",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH": "false",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "1",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "2",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_REQUIRE_RECONCILE_PASS": "false",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "90",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT": "3",
            "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13": (
                "false"
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED": "true",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED": "true",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH": "false",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "2",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "3",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_RECONCILE_PASS": "false",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_DATA_USABLE": "false",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_PARSER_VERIFIED": "false",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_WORKER_REGIME_REFRESH": "false",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_FAIL_CLOSED_ON_REGIME_REFRESH_ERROR": (
                "false"
            ),
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "45",
            "GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15": (
                "false"
            ),
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED": "true",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED": "true",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH": "false",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "2",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "3",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_RECONCILE_PASS": "false",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_PRIOR_EVENT_RECONCILE": "false",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_INDEX_ROUTING_GUARD": "false",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_WORKER_CONTEXT_REFRESH": "false",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_FAIL_CLOSED_ON_CONTEXT_REFRESH_ERROR": (
                "false"
            ),
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "75",
            "GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18": (
                "false"
            ),
        }
    )
    assert routing_settings.gateway_market_data_append_only_dry_run_enabled is True
    assert routing_settings.gateway_market_data_append_only_cutover_enabled is True
    assert (
        routing_settings.gateway_market_data_append_only_operating_mode
        == "MARKET_DATA_LIMITED"
    )
    assert (
        routing_settings.gateway_market_data_append_only_global_kill_switch
        is False
    )
    assert (
        routing_settings.gateway_market_data_append_only_auto_rollback_enabled
        is False
    )
    assert (
        routing_settings.gateway_market_data_append_only_global_max_skip_per_minute
        == 7
    )
    assert routing_settings.gateway_market_data_append_only_max_error_count == 1
    assert routing_settings.gateway_market_data_append_only_max_dead_letter_count == 2
    assert routing_settings.gateway_market_data_append_only_max_pending_within_sla == 80
    assert (
        routing_settings.gateway_market_data_append_only_max_condition_event_pending_within_sla
        == 4
    )
    assert (
        routing_settings.gateway_market_data_append_only_require_dashboard_fast_ok
        is False
    )
    assert (
        routing_settings.gateway_market_data_append_only_require_backlog_ready
        is False
    )
    assert (
        routing_settings.gateway_market_data_append_only_auto_rollback_cooldown_sec
        == 120
    )
    assert routing_settings.gateway_market_data_append_only_health_stale_sec == 30
    assert (
        routing_settings.gateway_market_data_append_only_price_tick_cutover_enabled
        is True
    )
    assert (
        routing_settings.gateway_market_data_append_only_tr_response_dry_run_enabled
        is True
    )
    assert (
        routing_settings.gateway_market_data_append_only_tr_response_cutover_enabled
        is True
    )
    assert (
        routing_settings.gateway_market_data_append_only_tr_response_require_worker_side_effects
        is False
    )
    assert routing_settings.gateway_market_data_append_only_cutover_event_types == (
        "price_tick",
    )
    assert routing_settings.gateway_market_data_append_only_require_reconcile_pass is False
    assert (
        routing_settings.gateway_market_data_append_only_require_latest_reconcile_pass
        is False
    )
    assert (
        routing_settings.gateway_market_data_append_only_require_worker_apply_enabled
        is False
    )
    assert (
        routing_settings.gateway_market_data_append_only_fail_closed_on_routing_error
        is False
    )
    assert (
        routing_settings.gateway_market_data_append_only_price_tick_max_skip_per_minute
        == 3
    )
    assert routing_settings.gateway_market_data_append_only_reconcile_max_age_sec == 60
    assert routing_settings.gateway_market_data_append_only_event_types == (
        "price_tick",
        "tr_response",
    )
    assert routing_settings.gateway_market_data_append_only_min_outbox_status == "ENQUEUED"
    assert routing_settings.gateway_market_reference_append_only_dry_run_enabled is True
    assert routing_settings.gateway_market_reference_append_only_cutover_enabled is True
    assert (
        routing_settings.gateway_market_reference_append_only_global_kill_switch
        is False
    )
    assert (
        routing_settings.gateway_market_reference_append_only_max_skip_per_minute
        == 1
    )
    assert (
        routing_settings.gateway_market_reference_append_only_max_pending_within_sla
        == 2
    )
    assert (
        routing_settings.gateway_market_reference_append_only_require_reconcile_pass
        is False
    )
    assert (
        routing_settings.gateway_market_reference_append_only_reconcile_max_age_sec
        == 90
    )
    assert routing_settings.gateway_market_reference_append_only_min_membership_count == 3
    assert (
        routing_settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
        is False
    )
    assert routing_settings.gateway_market_index_append_only_dry_run_enabled is True
    assert routing_settings.gateway_market_index_append_only_cutover_enabled is True
    assert routing_settings.gateway_market_index_append_only_global_kill_switch is False
    assert routing_settings.gateway_market_index_append_only_max_skip_per_minute == 2
    assert routing_settings.gateway_market_index_append_only_max_pending_within_sla == 3
    assert routing_settings.gateway_market_index_append_only_require_reconcile_pass is False
    assert routing_settings.gateway_market_index_append_only_require_data_usable is False
    assert routing_settings.gateway_market_index_append_only_require_parser_verified is False
    assert (
        routing_settings.gateway_market_index_append_only_require_worker_regime_refresh
        is False
    )
    assert (
        routing_settings.gateway_market_index_append_only_fail_closed_on_regime_refresh_error
        is False
    )
    assert routing_settings.gateway_market_index_append_only_reconcile_max_age_sec == 45
    assert (
        routing_settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
        is False
    )
    assert routing_settings.gateway_market_regime_append_only_dry_run_enabled is True
    assert routing_settings.gateway_market_regime_append_only_cutover_enabled is True
    assert routing_settings.gateway_market_regime_append_only_global_kill_switch is False
    assert routing_settings.gateway_market_regime_append_only_max_skip_per_minute == 2
    assert routing_settings.gateway_market_regime_append_only_max_pending_within_sla == 3
    assert (
        routing_settings.gateway_market_regime_append_only_require_reconcile_pass
        is False
    )
    assert (
        routing_settings.gateway_market_regime_append_only_require_prior_event_reconcile
        is False
    )
    assert (
        routing_settings.gateway_market_regime_append_only_require_index_routing_guard
        is False
    )
    assert (
        routing_settings.gateway_market_regime_append_only_require_worker_context_refresh
        is False
    )
    assert (
        routing_settings.gateway_market_regime_append_only_fail_closed_on_context_refresh_error
        is False
    )
    assert routing_settings.gateway_market_regime_append_only_reconcile_max_age_sec == 75
    assert (
        routing_settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18
        is False
    )

    try:
        load_settings({"MARKET_DATA_PROJECTION_RECONCILE_LIMIT": "0"})
    except ValueError as exc:
        assert "MARKET_DATA_PROJECTION_RECONCILE_LIMIT" in str(exc)
    else:
        raise AssertionError("expected invalid market data reconcile limit")
    invalid_market_reference_cases = {
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "0",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "-1",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "0",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT": "-1",
    }
    for key, value in invalid_market_reference_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid market reference setting for {key}")
    invalid_market_index_cases = {
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "0",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "-1",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "0",
    }
    for key, value in invalid_market_index_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid market index setting for {key}")

    invalid_operator_cases = {
        "OPERATOR_SQLITE_LOCK_RETRY_ATTEMPTS": "0",
        "OPERATOR_RUN_ONCE_LOCKED_HTTP_STATUS": "201",
        "OPS_SCRIPT_LOCKED_RETRY_ATTEMPTS": "0",
        "OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC": "-0.1",
        "OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC": "-0.1",
        "OPERATOR_SQLITE_BUSY_TIMEOUT_MS": "0",
        "OPS_SCRIPT_LOCKED_RETRY_SLEEP_SEC": "-0.1",
    }
    for key, value in invalid_operator_cases.items():
        try:
            load_settings({key: value})
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected invalid operator lock setting for {key}")
    try:
        load_settings(
            {
                "OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC": "1",
                "OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC": "0.5",
            }
        )
    except ValueError as exc:
        assert "OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC" in str(exc)
    else:
        raise AssertionError("expected invalid operator lock retry sleep bounds")

    try:
        load_settings({"GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "0"})
    except ValueError as exc:
        assert "GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC" in str(exc)
    else:
        raise AssertionError("expected invalid append-only routing max age")
    try:
        load_settings(
            {"GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE": "-1"}
        )
    except ValueError as exc:
        assert (
            "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE"
            in str(exc)
        )
    else:
        raise AssertionError("expected invalid condition_event skip budget")
    try:
        load_settings(
            {"GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_PAYLOAD_AGE_SEC": "0"}
        )
    except ValueError as exc:
        assert (
            "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_PAYLOAD_AGE_SEC"
            in str(exc)
        )
    else:
        raise AssertionError("expected invalid condition_event max payload age")
    invalid_controller_cases = {
        "GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE": "LIVE",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE": "-1",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_ERROR_COUNT": "-1",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_DEAD_LETTER_COUNT": "-1",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "-1",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_MAX_CONDITION_EVENT_PENDING_WITHIN_SLA": "-1",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_COOLDOWN_SEC": "0",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_HEALTH_STALE_SEC": "0",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_EVENT_AGE_SEC": "0",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_FUTURE_SKEW_SEC": "-1",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_GATEWAY_HEALTH_MAX_AGE_SEC": "0",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_RECONCILE_MAX_AGE_SEC": "0",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE": "-1",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_PENDING_WITHIN_SLA": "0",
        "MARKET_CONTEXT_SNAPSHOT_STALE_SEC": "0",
    }
    for key, value in invalid_controller_cases.items():
        try:
            load_settings({key: value})
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected invalid controller setting for {key}")


def test_realtime_subscription_settings_are_validated() -> None:
    invalid_cases = {
        "REALTIME_SUBSCRIPTION_MAX_TOTAL": "1",
        "REALTIME_SUBSCRIPTION_MAX_PER_THEME": "0",
        "REALTIME_SUBSCRIPTION_ANCHOR_CODES": "005930,BAD",
        "REALTIME_SUBSCRIPTION_REMOVE_STALE_AFTER_SEC": "10",
        "REALTIME_SUBSCRIPTION_EXCHANGE": "MIXED",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected invalid realtime subscription setting for {key}")


def test_theme_ratio_settings_are_validated() -> None:
    try:
        load_settings({"THEME_MIN_FRESH_COVERAGE_RATIO": "1.5"})
    except ValueError as exc:
        assert "ratio between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected invalid theme ratio configuration")

    try:
        load_settings({"THEME_MIN_OBSERVABLE_MEMBERS": "0"})
    except ValueError as exc:
        assert "THEME_MIN_OBSERVABLE_MEMBERS" in str(exc)
    else:
        raise AssertionError("expected invalid observable member minimum")


def test_observable_coverage_and_candidate_stale_settings_parse_env() -> None:
    settings = load_settings(
        {
            "THEME_OBSERVABLE_COVERAGE_ENABLED": "false",
            "THEME_MIN_OBSERVABLE_MEMBERS": "5",
            "CANDIDATE_TICK_STALE_SEC": "120",
            "CANDIDATE_STALE_REQUIRES_TICK_STALE": "false",
        }
    )

    assert settings.theme_observable_coverage_enabled is False
    assert settings.theme_min_observable_members == 5
    assert settings.candidate_tick_stale_sec == 120
    assert settings.candidate_stale_requires_tick_stale is False


def test_market_scan_settings_are_validated() -> None:
    invalid_cases = {
        "MARKET_SCAN_INTERVAL_SEC": "0",
        "MARKET_SCAN_TOP_N": "0",
        "MARKET_SCAN_TR_CODES": "TRADE_VALUE=OPT10032",
        "MARKET_SCAN_MARKETS": "KOSPI,NASDAQ",
        "MARKET_SCAN_MARKET_CODES": "KOSPI=001",
        "THEME_SNAPSHOT_STALE_SEC": "0",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected invalid market scan setting for {key}")


def test_naver_theme_import_settings_are_validated() -> None:
    invalid_cases = {
        "NAVER_THEME_IMPORT_TIMEOUT_SECONDS": "0",
        "NAVER_THEME_IMPORT_MAX_THEMES": "0",
        "NAVER_THEME_IMPORT_REQUEST_SLEEP_SECONDS": "-0.1",
        "NAVER_THEME_IMPORT_MIN_MEMBER_COUNT": "0",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid naver theme import setting: {key}")


def test_candidate_settings_are_validated() -> None:
    try:
        load_settings({"CONDITION_FUSION_SWEEP_INTERVAL_SEC": "0"})
    except ValueError as exc:
        assert "CONDITION_FUSION_SWEEP_INTERVAL_SEC" in str(exc)
    else:
        raise AssertionError("expected invalid condition fusion sweep interval")

    try:
        load_settings({"CANDIDATE_SOURCE_STALE_SEC": "0"})
    except ValueError as exc:
        assert "CANDIDATE_SOURCE_STALE_SEC" in str(exc)
    else:
        raise AssertionError("expected invalid candidate stale setting")

    try:
        load_settings({"CANDIDATE_THEME_SOURCE_STATES": "LEADING,"})
    except ValueError as exc:
        assert "CANDIDATE_THEME_SOURCE_STATES" in str(exc)
    else:
        raise AssertionError("expected invalid candidate list setting")


def test_strategy_settings_are_validated() -> None:
    try:
        load_settings({"STRATEGY_ENGINE_MAX_CANDIDATES": "0"})
    except ValueError as exc:
        assert "STRATEGY_ENGINE_MAX_CANDIDATES" in str(exc)
    else:
        raise AssertionError("expected invalid strategy max candidates setting")

    try:
        load_settings({"STRATEGY_ENGINE_ALLOWED_CANDIDATE_STATES": "CONTEXT_READY,"})
    except ValueError as exc:
        assert "STRATEGY_ENGINE_ALLOWED_CANDIDATE_STATES" in str(exc)
    else:
        raise AssertionError("expected invalid strategy state list setting")

    try:
        load_settings({"STRATEGY_PULLBACK_MIN_PCT": "5", "STRATEGY_PULLBACK_MAX_PCT": "1"})
    except ValueError as exc:
        assert "STRATEGY_PULLBACK_MAX_PCT" in str(exc)
    else:
        raise AssertionError("expected invalid strategy pullback range")

    try:
        load_settings({"STRATEGY_FOLLOWER_EXPANSION_MIN_THEME_RISING_RATIO": "1.5"})
    except ValueError as exc:
        assert "ratio between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected invalid strategy follower ratio")


def test_risk_gate_settings_are_validated() -> None:
    try:
        load_settings({"RISK_GATE_MAX_STRATEGY_OBSERVATIONS": "0"})
    except ValueError as exc:
        assert "RISK_GATE_MAX_STRATEGY_OBSERVATIONS" in str(exc)
    else:
        raise AssertionError("expected invalid risk strategy observation limit")

    try:
        load_settings({"RISK_GATE_MIN_THEME_RISING_RATIO": "1.2"})
    except ValueError as exc:
        assert "ratio between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected invalid risk theme ratio")

    try:
        load_settings({"RISK_GATE_MAX_CHANGE_RATE": "-1"})
    except ValueError as exc:
        assert "RISK_GATE_MAX_CHANGE_RATE" in str(exc)
    else:
        raise AssertionError("expected invalid risk negative setting")

    try:
        load_settings({"RISK_CROSS_EXCHANGE_DIVERGENCE_BP": "-1"})
    except ValueError as exc:
        assert "RISK_CROSS_EXCHANGE_DIVERGENCE_BP" in str(exc)
    else:
        raise AssertionError("expected invalid cross-exchange risk threshold")


def test_entry_timing_settings_are_validated() -> None:
    invalid_cases = {
        "ENTRY_TIMING_MAX_PLANS_PER_RUN": "0",
        "ENTRY_TIMING_PLAN_TTL_SECONDS": "0",
        "ENTRY_TIMING_MAX_SPREAD_TICKS": "0",
        "ENTRY_TIMING_ALLOW_MARKET_ORDER": "true",
        "ENTRY_TIMING_PRICE_OFFSET_TICKS": "-1",
        "ENTRY_TIMING_STALE_MAX_SECONDS": "0",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid entry timing setting: {key}")

    try:
        load_settings(
            {
                "ENTRY_TIMING_PULLBACK_MIN_PCT": "5",
                "ENTRY_TIMING_PULLBACK_MAX_PCT": "1",
            }
        )
    except ValueError as exc:
        assert "ENTRY_TIMING_PULLBACK_MAX_PCT" in str(exc)
    else:
        raise AssertionError("expected invalid entry timing pullback range")

    try:
        load_settings(
            {
                "ENTRY_TIMING_DEFAULT_NOTIONAL": "200000",
                "ENTRY_TIMING_MAX_NOTIONAL": "100000",
            }
        )
    except ValueError as exc:
        assert "ENTRY_TIMING_DEFAULT_NOTIONAL" in str(exc)
    else:
        raise AssertionError("expected invalid entry timing notional range")


def test_live_sim_order_plan_settings_are_validated() -> None:
    invalid_cases = {
        "LIVE_SIM_ORDER_PLAN_STALE_SEC": "0",
        "LIVE_SIM_ORDER_PLAN_MAX_PLANS_PER_RUN": "0",
        "LIVE_SIM_ORDER_PLAN_MAX_COMMANDS_PER_RUN": "0",
        "LIVE_SIM_ORDER_PLAN_MAX_PRICE_DRIFT_PCT": "-0.1",
        "LIVE_SIM_ORDER_PLAN_ALLOW_MARKET_ORDER": "true",
        "LIVE_SIM_ORDER_PLAN_ALLOWED_SIDE": "SELL",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid LIVE_SIM order plan setting: {key}")

    try:
        load_settings(
            {
                "LIVE_SIM_ORDER_PLAN_MIN_NOTIONAL": "200000",
                "LIVE_SIM_ORDER_PLAN_MAX_NOTIONAL": "100000",
            }
        )
    except ValueError as exc:
        assert "LIVE_SIM_ORDER_PLAN_MIN_NOTIONAL" in str(exc)
    else:
        raise AssertionError("expected invalid LIVE_SIM order plan min/max notional")

    try:
        load_settings(
            {
                "LIVE_SIM_ORDER_PLAN_DEFAULT_NOTIONAL": "200000",
                "LIVE_SIM_ORDER_PLAN_MAX_NOTIONAL": "100000",
            }
        )
    except ValueError as exc:
        assert "LIVE_SIM_ORDER_PLAN_DEFAULT_NOTIONAL" in str(exc)
    else:
        raise AssertionError("expected invalid LIVE_SIM order plan default/max notional")


def test_live_sim_order_exchange_settings_are_validated() -> None:
    nxt = load_settings({"LIVE_SIM_ORDER_EXCHANGE": "nxt"})
    sor = load_settings(
        {
            "LIVE_SIM_ORDER_EXCHANGE": "all",
            "LIVE_SIM_NXT_SUPPORT_CONFIRMED": "true",
        }
    )

    assert nxt.live_sim_order_exchange == "NXT"
    assert nxt.live_sim_nxt_support_confirmed is False
    assert sor.live_sim_order_exchange == "SOR"
    assert sor.live_sim_nxt_support_confirmed is True

    try:
        load_settings({"LIVE_SIM_ORDER_EXCHANGE": "ATS"})
    except ValueError as exc:
        assert "LIVE_SIM_ORDER_EXCHANGE" in str(exc)
    else:
        raise AssertionError("expected invalid LIVE_SIM order exchange setting")


def test_live_sim_buy_reprice_and_reconcile_settings_are_validated() -> None:
    settings = load_settings(
        {
            "LIVE_SIM_BUY_PRICE_OFFSET_TICKS": "3",
            "LIVE_SIM_REPRICE_ENABLED": "true",
            "LIVE_SIM_REPRICE_MAX_ATTEMPTS": "2",
            "LIVE_SIM_RECONCILE_NOTIONAL_TOLERANCE": "0.5",
        }
    )

    assert settings.live_sim_buy_price_offset_ticks == 3
    assert settings.live_sim_reprice_enabled is True
    assert settings.live_sim_reprice_max_attempts == 2
    assert settings.live_sim_reconcile_notional_tolerance == 0.5

    invalid_cases = {
        "LIVE_SIM_BUY_PRICE_OFFSET_TICKS": "4",
        "LIVE_SIM_REPRICE_MAX_ATTEMPTS": "0",
        "LIVE_SIM_RECONCILE_NOTIONAL_TOLERANCE": "-0.1",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid LIVE_SIM setting: {key}")


def test_live_sim_daily_loss_settings_are_validated() -> None:
    settings = load_settings(
        {
            "LIVE_SIM_MAX_DAILY_LOSS": "100000",
            "LIVE_SIM_MAX_DAILY_LOSS_PCT": "5.5",
        }
    )

    assert settings.live_sim_max_daily_loss == 100_000
    assert settings.live_sim_max_daily_loss_pct == 5.5

    for key in ("LIVE_SIM_MAX_DAILY_LOSS", "LIVE_SIM_MAX_DAILY_LOSS_PCT"):
        try:
            load_settings({key: "-0.1"})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid LIVE_SIM daily loss setting: {key}")


def test_live_sim_entry_window_settings_are_validated() -> None:
    normalized = load_settings(
        {
            "LIVE_SIM_ENTRY_WINDOW_START": "9:5:0",
            "LIVE_SIM_ENTRY_WINDOW_END": "14:30:00",
        }
    )
    assert normalized.live_sim_entry_window_start == "09:05:00"

    invalid_cases = {
        "LIVE_SIM_ENTRY_WINDOW_START": "090500",
        "LIVE_SIM_ENTRY_WINDOW_END": "25:00:00",
        "LIVE_SIM_EXIT_EOD_FLATTEN_TIME": "151500",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid LIVE_SIM time setting: {key}")

    for env in (
        {
            "LIVE_SIM_ENTRY_WINDOW_START": "14:30:00",
            "LIVE_SIM_ENTRY_WINDOW_END": "09:05:00",
        },
        {
            "LIVE_SIM_ENTRY_WINDOW_END": "15:15:00",
            "LIVE_SIM_EXIT_EOD_FLATTEN_TIME": "15:15:00",
        },
        {
            "LIVE_SIM_ENTRY_WINDOW_END": "15:20:00",
            "LIVE_SIM_EXIT_EOD_FLATTEN_TIME": "15:15:00",
        },
    ):
        try:
            load_settings(env)
        except ValueError as exc:
            assert "LIVE_SIM_ENTRY_WINDOW" in str(exc)
        else:
            raise AssertionError("expected invalid LIVE_SIM entry window relationship")


def test_live_sim_operating_loop_settings_are_validated() -> None:
    settings = load_settings(
        {
            "LIVE_SIM_OPERATING_LOOP_ENABLED": "true",
            "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS": "true",
            "LIVE_SIM_OPERATING_LOOP_INTERVAL_SEC": "5",
            "LIVE_SIM_OPERATING_LOOP_MARKET_OPEN_TIME": "9:5:0",
            "LIVE_SIM_OPERATING_LOOP_MARKET_CLOSE_TIME": "15:20:00",
        }
    )

    assert settings.live_sim_operating_loop_enabled is True
    assert settings.live_sim_operating_loop_queue_commands is True
    assert settings.live_sim_operating_loop_interval_sec == 5
    assert settings.live_sim_operating_loop_market_open_time == "09:05:00"
    assert settings.live_sim_operating_loop_market_close_time == "15:20:00"

    invalid_cases = {
        "LIVE_SIM_OPERATING_LOOP_INTERVAL_SEC": "4",
        "LIVE_SIM_OPERATING_LOOP_MARKET_OPEN_TIME": "090500",
        "LIVE_SIM_OPERATING_LOOP_MARKET_CLOSE_TIME": "25:20:00",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid LIVE_SIM operating loop setting: {key}")


def test_dashboard_settings_are_validated() -> None:
    invalid_cases = [
        ({"DASHBOARD_REFRESH_SEC": "0"}, "DASHBOARD_REFRESH_SEC"),
        (
            {
                "DASHBOARD_SNAPSHOT_DEFAULT_LIMIT": "201",
                "DASHBOARD_MAX_LIMIT": "200",
            },
            "DASHBOARD_SNAPSHOT_DEFAULT_LIMIT",
        ),
        (
            {"DASHBOARD_SNAPSHOT_FAST_CACHE_TTL_SEC": "-1"},
            "DASHBOARD_SNAPSHOT_FAST_CACHE_TTL_SEC",
        ),
        (
            {"DASHBOARD_SNAPSHOT_FAST_DEFAULT_LIMIT": "0"},
            "DASHBOARD_SNAPSHOT_FAST_DEFAULT_LIMIT",
        ),
        (
            {"DASHBOARD_SNAPSHOT_FAST_TIMEOUT_BUDGET_MS": "99"},
            "DASHBOARD_SNAPSHOT_FAST_TIMEOUT_BUDGET_MS",
        ),
        (
            {"DASHBOARD_SNAPSHOT_WARN_LATENCY_MS": "0"},
            "DASHBOARD_SNAPSHOT_WARN_LATENCY_MS",
        ),
        (
            {
                "DASHBOARD_SNAPSHOT_WARN_LATENCY_MS": "3000",
                "DASHBOARD_SNAPSHOT_FAIL_LATENCY_MS": "2000",
            },
            "DASHBOARD_SNAPSHOT_FAIL_LATENCY_MS",
        ),
    ]
    for env, expected in invalid_cases:
        try:
            load_settings(env)
        except ValueError as exc:
            assert expected in str(exc)
        else:
            raise AssertionError(f"expected invalid dashboard setting: {expected}")


def test_dry_run_oms_settings_are_validated() -> None:
    invalid_cases = {
        "DRY_RUN_ORDER_ROUTING_ENABLED": "true",
        "DRY_RUN_GATEWAY_COMMAND_ENABLED": "true",
        "DRY_RUN_ALLOW_SHORT": "true",
        "DRY_RUN_MIN_QUANTITY": "0",
        "DRY_RUN_MAX_DAILY_INTENTS": "0",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid dry-run setting: {key}")

    try:
        load_settings(
            {
                "DRY_RUN_DEFAULT_POSITION_NOTIONAL": "2000000",
                "DRY_RUN_MAX_POSITION_NOTIONAL": "1000000",
            }
        )
    except ValueError as exc:
        assert "DRY_RUN_DEFAULT_POSITION_NOTIONAL" in str(exc)
    else:
        raise AssertionError("expected invalid dry-run notional range")


def test_dry_run_exit_settings_are_validated() -> None:
    invalid_cases = {
        "DRY_RUN_EXIT_ORDER_ROUTING_ENABLED": "true",
        "DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED": "true",
        "DRY_RUN_EXIT_ALLOW_SHORT": "true",
        "DRY_RUN_EXIT_ALLOW_SELL_CLOSE_ONLY": "false",
        "DRY_RUN_EXIT_STOP_LOSS_PCT": "0",
        "DRY_RUN_EXIT_TAKE_PROFIT_PCT": "0",
        "DRY_RUN_EXIT_TRAILING_STOP_PCT": "0",
        "DRY_RUN_EXIT_MAX_HOLD_SEC": "0",
        "DRY_RUN_EXIT_STALE_TICK_SEC": "0",
        "DRY_RUN_EXIT_INTENT_TTL_SEC": "0",
    }
    for key, value in invalid_cases.items():
        try:
            load_settings({key: value})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected invalid dry-run exit setting: {key}")

    try:
        load_settings({"DRY_RUN_EXIT_MIN_HOLD_SEC": "-1"})
    except ValueError as exc:
        assert "DRY_RUN_EXIT_MIN_HOLD_SEC" in str(exc)
    else:
        raise AssertionError("expected invalid dry-run exit min hold setting")


def test_ai_context_settings_are_validated() -> None:
    try:
        load_settings(
            {
                "AI_SIDECAR_CONTEXT_DEFAULT_LIMIT": "201",
                "AI_SIDECAR_CONTEXT_MAX_LIMIT": "200",
            }
        )
    except ValueError as exc:
        assert "AI_SIDECAR_CONTEXT_DEFAULT_LIMIT" in str(exc)
    else:
        raise AssertionError("expected invalid AI context limit setting")

    for field_name in ("AI_SIDECAR_TOOLS_ENABLED", "AI_SIDECAR_ORDER_TOOLS_ENABLED"):
        try:
            load_settings({field_name: "true"})
        except ValueError as exc:
            assert field_name in str(exc)
        else:
            raise AssertionError(f"expected invalid {field_name} setting")

    try:
        load_settings({"AI_SIDECAR_MAX_RETRIES": "4"})
    except ValueError as exc:
        assert "AI_SIDECAR_MAX_RETRIES" in str(exc)
    else:
        raise AssertionError("expected invalid AI retry setting")
