from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]


def test_start_market_open_observe_script_keeps_order_flags_off() -> None:
    script = (ROOT_DIR / "tools" / "start_market_open_observe.ps1").read_text(encoding="utf-8")

    assert '$env:TRADING_MODE = "OBSERVE"' in script
    assert '$env:TRADING_ALLOW_LIVE_REAL = "false"' in script
    assert '$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"' in script
    assert '$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"' in script
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
    assert 'queue_commands = "true"' not in script.lower()


def test_stop_core_gateway_script_targets_core_gateway_processes_only() -> None:
    script = (ROOT_DIR / "tools" / "stop_core_gateway.ps1").read_text(encoding="utf-8")
    lowered = script.lower()

    assert "get-ciminstance -classname win32_process" in lowered
    assert "apps\\.core_api:app" in script
    assert "apps\\.mock_gateway" in script
    assert "apps\\.kiwoom_gateway" in script
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
    assert "KIWOOM_MARKET_INDEX_ENABLED" in script
    assert "Market index adapter:" in script
    assert '"--market-index-enabled"' in script
    assert '"--market-index-realtime-enabled"' in script
    assert '"--market-index-codes"' in script
    assert '"--no-market-index-tr-bootstrap-enabled"' in script
