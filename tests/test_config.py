from gateway.settings import load_gateway_settings
from services.config import TradingMode, load_settings


def test_default_settings_are_observe_with_live_flags_disabled() -> None:
    settings = load_settings({})

    assert settings.trading_mode is TradingMode.OBSERVE
    assert settings.live_sim_allowed is False
    assert settings.live_real_allowed is False
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
    assert settings.market_data_enabled is True
    assert settings.market_data_tick_stale_sec == 10
    assert settings.market_data_degraded_tick_stale_sec == 30
    assert settings.market_data_bar_intervals_sec == (60, 180, 300)
    assert settings.theme_service_enabled is True
    assert settings.theme_min_active_members == 2
    assert settings.theme_min_fresh_coverage_ratio == 0.3
    assert settings.theme_leading_rising_ratio == 0.5
    assert settings.theme_spreading_rising_ratio == 0.35
    assert settings.theme_import_allow_replace is False
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
    assert settings.dashboard_enabled is True
    assert settings.dashboard_refresh_sec == 5
    assert settings.dashboard_snapshot_default_limit == 50
    assert settings.dashboard_max_limit == 200
    assert settings.dashboard_show_raw_json is True
    assert settings.dashboard_route_enabled is True


def test_default_gateway_settings_are_mock_local_transport() -> None:
    settings = load_gateway_settings({})

    assert settings.core_url == "http://127.0.0.1:8000"
    assert settings.core_token == ""
    assert settings.source == "mock_gateway"
    assert settings.poll_interval_sec == 1.0
    assert settings.heartbeat_interval_sec == 2.0
    assert settings.event_timeout_sec == 5.0
    assert settings.command_wait_sec == 1.0
    assert settings.command_limit == 20
    assert settings.mock_once is False
    assert settings.mock_price_tick_interval_sec == 2.0


def test_market_data_interval_settings_are_validated() -> None:
    try:
        load_settings({"MARKET_DATA_BAR_INTERVALS_SEC": "60,90"})
    except ValueError as exc:
        assert "minute-aligned" in str(exc)
    else:
        raise AssertionError("expected invalid market data interval configuration")


def test_theme_ratio_settings_are_validated() -> None:
    try:
        load_settings({"THEME_MIN_FRESH_COVERAGE_RATIO": "1.5"})
    except ValueError as exc:
        assert "ratio between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected invalid theme ratio configuration")


def test_candidate_settings_are_validated() -> None:
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
