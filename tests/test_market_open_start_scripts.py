from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]


def test_start_market_open_observe_script_keeps_order_flags_off() -> None:
    script = (ROOT_DIR / "tools" / "start_market_open_observe.ps1").read_text(encoding="utf-8")

    assert '$env:TRADING_MODE = "OBSERVE"' in script
    assert '$env:TRADING_ALLOW_LIVE_REAL = "false"' in script
    _assert_observe_side_effect_flags_are_overridden(script)
    assert "queue_commands default remains false" in script
    assert "Dashboard URL:" in script
    assert "/api/gateway/events/recent?limit=20" in script
    assert "--realtime-exchange $RealtimeExchange" in script
    assert "ConditionProfilesFile" in script
    assert "ConditionProfilesJson" in script
    assert "KIWOOM_CONDITION_PROFILES_FILE" in script
    assert "market_open_profiles.json" in script
    assert "$env:KIWOOM_CONDITION_PROFILES = $ResolvedConditionProfiles" in script
    assert "--condition-profiles `$env:KIWOOM_CONDITION_PROFILES" in script
    assert "Condition mode: $ConditionMode" in script
    assert "MULTI_PROFILE" in script
    assert "LEGACY_SINGLE" in script
    assert "KIWOOM_MARKET_INDEX_ENABLED" in script
    assert "Market index adapter:" in script
    assert "--market-index-enabled" in script
    assert "--market-index-realtime-enabled" in script
    assert "--market-index-codes" in script
    assert "--no-market-index-tr-bootstrap-enabled" in script
    assert "RunAll" in script
    assert "RunGateway" in script
    assert "MarketReferenceProjectionValidation" in script
    assert '$env:PROJECTION_OUTBOX_WORKER_ENABLED = "false"' in script
    assert '$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED = "false"' in script
    assert (
        '$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED = if '
        '($MarketReferenceProjectionValidation) { "true" } else { "false" }'
    ) in script
    assert (
        '$env:PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED = if '
        '($MarketReferenceProjectionValidation) { "true" } else { "false" }'
    ) in script
    assert (
        '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED = if '
        '($MarketReferenceProjectionValidation) { "true" } else { "false" }'
    ) in script
    assert '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED = "false"' in script
    assert (
        '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13 = "true"'
        in script
    )
    assert '$env:CONDITION_FUSION_SWEEP_ENABLED = "false"' in script
    assert '$env:INCREMENTAL_EVALUATION_WORKER_ENABLED = "false"' in script
    assert '$env:EVENT_STORE_RETENTION_ENABLED = "false"' in script
    assert (
        '"INCREMENTAL_EVALUATION_WORKER_ENABLED='
        '$($env:INCREMENTAL_EVALUATION_WORKER_ENABLED)"'
    ) in script
    assert "RunThemeRefreshLoop" in script
    assert "start_kiwoom_gateway_visible.ps1" in script
    assert "start_theme_refresh_loop.ps1" in script
    assert "Start-DetachedRuntimeProcess" in script
    assert 'WindowStyle = "Hidden"' in script
    assert '$env:MARKET_SCAN_ENABLED = "true"' in script
    assert "One-shot launcher command:" in script
    assert "ThemeRefreshTradingSession" in script
    assert '"NXT"' in script
    assert "-TradingSession" in script
    assert 'queue_commands = "true"' not in script.lower()


def test_start_theme_refresh_loop_uses_market_scan_interval_and_order_guard() -> None:
    script = (ROOT_DIR / "tools" / "start_theme_refresh_loop.ps1").read_text(encoding="utf-8")

    assert "/api/themes/refresh-cycle/run-once" in script
    assert "MARKET_SCAN_INTERVAL_SEC" in script
    assert "THEME_REFRESH_TRADING_SESSION" in script
    assert "TradingSession" in script
    assert '"NXT"' in script
    assert '"08:00:00"' in script
    assert '"20:00:00"' in script
    assert '"KRX"' in script
    assert '"09:00:00"' in script
    assert '"15:30:00"' in script
    assert "queue_market_scan_commands" in script
    assert "queue_realtime_commands" in script
    assert "order_command_delta" in script
    assert "no_order_side_effects" in script
    assert "Start-Sleep" in script
    assert "X-Core-Token" in script


def test_stop_core_gateway_script_targets_core_gateway_and_theme_refresh_loop() -> None:
    script = (ROOT_DIR / "tools" / "stop_core_gateway.ps1").read_text(encoding="utf-8")
    lowered = script.lower()

    assert "get-ciminstance -classname win32_process" in lowered
    assert "apps\\.core_api:app" in script
    assert "apps\\.mock_gateway" in script
    assert "apps\\.kiwoom_gateway" in script
    assert "ThemeRefreshOnly" in script
    assert "start_theme_refresh_loop\\.ps1" in script
    assert "theme_refresh_loop" in script
    assert "parentprocessid" in lowered
    assert 'conhost.exe' in lowered
    assert "$pscmdlet.shouldprocess" in lowered
    assert "stop-process" in lowered
    assert "get-process python" not in lowered


def test_start_kiwoom_gateway_visible_defaults_to_multi_profile_file() -> None:
    script = (ROOT_DIR / "tools" / "start_kiwoom_gateway_visible.ps1").read_text(
        encoding="utf-8"
    )

    assert "KIWOOM_CONDITION_PROFILES_FILE" in script
    assert "configs\\condition_profiles\\market_open_profiles.json" in script
    assert "Condition mode: $ConditionMode" in script
    assert "MULTI_PROFILE" in script
    assert '"LEGACY_SINGLE"' in script
    assert '"--condition-profiles"' in script
    assert '"--condition-name"' in script
    assert '"--poll-wait-sec", $env:GATEWAY_COMMAND_WAIT_SEC' in script
    assert "KIWOOM_MARKET_INDEX_ENABLED" in script
    assert "Market index adapter:" in script
    assert '"--market-index-enabled"' in script
    assert '"--market-index-realtime-enabled"' in script
    assert '"--market-index-codes"' in script
    assert '"--no-market-index-tr-bootstrap-enabled"' in script
    _assert_observe_side_effect_flags_are_overridden(script)


def _assert_observe_side_effect_flags_are_overridden(script: str) -> None:
    false_flags = (
        "DRY_RUN_ORDER_ROUTING_ENABLED",
        "DRY_RUN_GATEWAY_COMMAND_ENABLED",
        "DRY_RUN_EXIT_ENGINE_ENABLED",
        "DRY_RUN_EXIT_INTENT_CREATION_ENABLED",
        "DRY_RUN_EXIT_ORDER_CREATION_ENABLED",
        "DRY_RUN_EXIT_SIMULATED_FILL_ENABLED",
        "DRY_RUN_EXIT_ORDER_ROUTING_ENABLED",
        "DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED",
        "LIVE_SIM_ENABLED",
        "LIVE_SIM_ORDER_ROUTING_ENABLED",
        "LIVE_SIM_GATEWAY_COMMAND_ENABLED",
        "LIVE_SIM_ALLOW_BUY",
        "LIVE_SIM_ALLOW_SELL",
        "LIVE_SIM_ALLOW_EXIT_SELL",
        "LIVE_SIM_REPRICE_ENABLED",
        "LIVE_SIM_PILOT_PIPELINE_ENABLED",
        "LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND",
        "LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED",
        "LIVE_SIM_CANCEL_ENABLED",
        "LIVE_SIM_CANCEL_UNFILLED_ENABLED",
        "LIVE_SIM_EXIT_ENGINE_ENABLED",
        "LIVE_SIM_EXIT_ORDER_CREATION_ENABLED",
        "LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED",
        "LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED",
        "LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED",
        "LIVE_SIM_OPERATING_CYCLE_ENABLED",
        "LIVE_SIM_OPERATING_LOOP_ENABLED",
        "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS",
    )
    for flag in false_flags:
        assert f'$env:{flag} = "false"' in script
        assert f'"{flag}=$($env:{flag})"' in script

    for flag in ("LIVE_SIM_KILL_SWITCH", "LIVE_SIM_CANCEL_KILL_SWITCH"):
        assert f'$env:{flag} = "true"' in script
        assert f'"{flag}=$($env:{flag})"' in script
