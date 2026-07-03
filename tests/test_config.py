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
    assert settings.event_store_retention_enabled is False
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
    assert settings.theme_leading_rising_ratio == 0.5
    assert settings.theme_spreading_rising_ratio == 0.35
    assert settings.theme_import_allow_replace is False
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
    assert settings.candidate_fsm_enabled is True
    assert settings.candidate_trade_date_timezone == "Asia/Seoul"
    assert settings.candidate_source_stale_sec == 300
    assert settings.candidate_tick_stale_sec == 30
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
    assert settings.live_sim_operating_loop_enabled is False
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


def test_market_data_interval_settings_are_validated() -> None:
    try:
        load_settings({"MARKET_DATA_BAR_INTERVALS_SEC": "60,90"})
    except ValueError as exc:
        assert "minute-aligned" in str(exc)
    else:
        raise AssertionError("expected invalid market data interval configuration")


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


def test_live_sim_operating_loop_settings_are_validated() -> None:
    settings = load_settings(
        {
            "LIVE_SIM_OPERATING_LOOP_ENABLED": "true",
            "LIVE_SIM_OPERATING_LOOP_INTERVAL_SEC": "5",
            "LIVE_SIM_OPERATING_LOOP_MARKET_OPEN_TIME": "9:5:0",
            "LIVE_SIM_OPERATING_LOOP_MARKET_CLOSE_TIME": "15:20:00",
        }
    )

    assert settings.live_sim_operating_loop_enabled is True
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
    try:
        load_settings({"DASHBOARD_REFRESH_SEC": "0"})
    except ValueError as exc:
        assert "DASHBOARD_REFRESH_SEC" in str(exc)
    else:
        raise AssertionError("expected invalid dashboard refresh setting")

    try:
        load_settings(
            {
                "DASHBOARD_SNAPSHOT_DEFAULT_LIMIT": "201",
                "DASHBOARD_MAX_LIMIT": "200",
            }
        )
    except ValueError as exc:
        assert "DASHBOARD_SNAPSHOT_DEFAULT_LIMIT" in str(exc)
    else:
        raise AssertionError("expected invalid dashboard limit setting")


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
