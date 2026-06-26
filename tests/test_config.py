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
