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
    assert "MarketReferenceLimitedCutover" in script
    assert "RealtimeFidValidation" in script
    assert "DisableStockRealtime" in script
    assert "DisableMarketIndexRealtime" in script
    assert "AppendOnlyEvidence" in script
    assert "AllowOperatingDatabase" in script
    assert "MarketDataOperatingMode" in script
    assert "MarketDataGlobalSkipBudget" in script
    assert "MarketScanParserVerified" in script
    assert '"MARKET_SCAN_PARSER_STATUS=$($env:MARKET_SCAN_PARSER_STATUS)"' in script
    assert '"KOA_STUDIO_VERIFIED"' in script
    assert "$GatewayScriptParams.DisableConditions = $true" in script
    assert "$RealtimeFidValidation -or $DisableStockRealtime" in script
    assert 'throw "DbPath is required for realtime/evidence validation modes."' in script
    assert "Assert-CoreObserveSafety" in script
    assert '$MarketDataOperatingMode -eq "MARKET_DATA_FULL_GUARDED"' in script
    assert '"TRADING_DB_PATH=$($env:TRADING_DB_PATH)"' in script
    assert '"--reload"' not in script
    assert (
        '$env:PROJECTION_OUTBOX_WORKER_ENABLED = if '
        '($AppendOnlyEvidence) { "true" } else { "false" }'
    ) in script
    assert (
        '$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED = if '
        '($AppendOnlyEvidence) { "true" } else { "false" }'
    ) in script
    assert (
        '$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED = if '
        '($MarketReferenceValidationRequested) { "true" } else { "false" }'
    ) in script
    assert (
        '$env:PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED = if '
        '($MarketReferenceValidationRequested) { "true" } else { "false" }'
    ) in script
    assert (
        '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED = if '
        '($MarketReferenceValidationRequested) { "true" } else { "false" }'
    ) in script
    assert (
        '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED = if '
        '($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) '
        '{ "true" } else { "false" }'
    ) in script
    assert (
        '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH = if '
        '($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) '
        '{ "false" } else { "true" }'
    ) in script
    assert (
        '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE = if '
        '($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) '
        '{ "1" } else { "0" }'
    ) in script
    assert (
        '$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13 = if '
        '($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) '
        '{ "false" } else { "true" }'
        in script
    )
    _assert_append_only_evidence_flags_are_guarded(script)
    assert '$env:CONDITION_FUSION_SWEEP_ENABLED = "false"' in script
    assert '$env:INCREMENTAL_EVALUATION_WORKER_ENABLED = "false"' in script
    assert '$env:EVENT_STORE_RETENTION_ENABLED = "false"' in script
    assert (
        '"INCREMENTAL_EVALUATION_WORKER_ENABLED='
        '$($env:INCREMENTAL_EVALUATION_WORKER_ENABLED)"'
    ) in script
    assert "RunThemeRefreshLoop" in script
    assert "start_kiwoom_gateway_visible.ps1" in script
    assert "$GatewayScriptParams.ClearRealtimeOnLogin = $true" in script
    assert "start_theme_refresh_loop.ps1" in script
    assert "Start-DetachedRuntimeProcess" in script
    assert 'WindowStyle = "Hidden"' in script
    assert '$env:MARKET_SCAN_ENABLED = "true"' in script
    assert "One-shot launcher command:" in script
    assert "ThemeRefreshTradingSession" in script
    assert '"NXT"' in script
    assert "-TradingSession" in script
    assert 'queue_commands = "true"' not in script.lower()


def test_append_only_daily_evidence_wrappers_require_persistent_safe_runtime() -> None:
    start_script = (
        ROOT_DIR / "tools" / "start_append_only_daily_evidence.ps1"
    ).read_text(encoding="utf-8")
    close_script = (
        ROOT_DIR / "tools" / "close_append_only_daily_evidence.ps1"
    ).read_text(encoding="utf-8")

    assert "KrxTradingDayConfirmed is required" in start_script
    assert "Historical or future evidence start is forbidden" in start_script
    assert "Persistent 10-day evidence DB cannot be stored under TEMP" in start_script
    assert "append-only-10day.sqlite3" in start_script
    assert 'MarketDataOperatingMode = "MARKET_DATA_FULL_GUARDED"' in start_script
    assert 'ThemeRefreshTradingSession = "KRX"' in start_script
    assert 'ThemeRefreshQueueRealtimeCommands = "false"' in start_script
    assert "MarketScanParserVerified = $true" in start_script
    assert "$CoreParameters.RunCore = $true" in start_script
    assert "$GatewayParameters.RunGateway = $true" in start_script
    assert "$ThemeParameters.RunThemeRefreshLoop = $true" in start_script
    assert "GatewayStabilizeSec" in start_script
    assert "GatewayStartAttempts" in start_script
    assert "Gateway did not stabilize; retrying after 5 seconds" in start_script
    assert "append-only-daily-session/v1" in start_script
    assert "failed_command_count" in start_script
    assert '$ThemeRefreshQueueRealtimeCommands = "false"' in (
        ROOT_DIR / "tools" / "start_market_open_observe.ps1"
    ).read_text(encoding="utf-8")

    assert "apps\\.kiwoom_gateway" in close_script
    assert "start_theme_refresh_loop\\.ps1" in close_script
    assert "ops_append_only_daily_evidence.py" in close_script
    assert '"--session-state-path", $SessionStatePath' in close_script
    assert "Daily evidence close failed. Core remains running" in close_script
    assert "Refusing to stop unexpected listener" in close_script
    assert "uvicorn apps\\.core_api:app" in close_script
    assert "Remove-Item -LiteralPath $ResolvedDbPath" not in close_script


def _assert_append_only_evidence_flags_are_guarded(script: str) -> None:
    enabled_flags = (
        "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED",
        "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED",
        "PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_DRY_RUN_ENABLED",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_DRY_RUN_ENABLED",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED",
        "LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED",
        "LIVE_SIM_LIFECYCLE_WORKER_ENABLED",
        "LIVE_SIM_LIFECYCLE_CUTOVER_DRY_RUN_ENABLED",
        "LIVE_SIM_LIFECYCLE_CUTOVER_ENABLED",
    )
    for flag in enabled_flags:
        expected = f'$env:{flag} = if ($AppendOnlyEvidence) {{ "true" }} else {{ "false" }}'
        assert expected in script

    budget_flags = (
        "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE",
    )
    for flag in budget_flags:
        expected = f'$env:{flag} = if ($AppendOnlyEvidence) {{ "1" }} else {{ "0" }}'
        assert expected in script

    assert (
        '$env:GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE = if '
        '($AppendOnlyEvidence) { [string]$MarketDataGlobalSkipBudget } else { "0" }'
    ) in script
    assert (
        "$env:GATEWAY_MARKET_DATA_APPEND_ONLY_"
        'CONDITION_EVENT_ALLOW_CANDIDATE_INGEST_IN_WORKER = "false"'
    ) in script


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
    assert "DisableConditions" in script
    assert "DisableRealtimeCodes" in script
    assert "ClearRealtimeOnLogin" in script
    assert '$env:KIWOOM_CONDITION_NAME = ""' in script
    assert '$env:KIWOOM_CONDITION_PROFILES_FILE = ""' in script
    assert '$env:KIWOOM_CONDITION_PROFILES = ""' in script
    assert '"KIWOOM_CONDITION_NAME="' in script
    assert '"KIWOOM_CONDITION_PROFILES_FILE="' in script
    assert '"KIWOOM_CONDITION_PROFILES="' in script
    assert '"KIWOOM_REALTIME_CODES="' in script
    assert '$env:KIWOOM_REALTIME_CODES = ""' in script
    assert '"--clear-realtime-on-login"' in script
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
