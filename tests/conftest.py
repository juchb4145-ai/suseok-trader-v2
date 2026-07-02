from __future__ import annotations

import pytest
from services.config import clear_settings_cache


@pytest.fixture(autouse=True)
def reset_settings_cache(monkeypatch: pytest.MonkeyPatch):
    clear_settings_cache()
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    yield
    clear_settings_cache()
