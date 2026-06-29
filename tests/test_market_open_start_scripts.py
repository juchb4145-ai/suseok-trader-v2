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
    assert 'queue_commands = "true"' not in script.lower()
