from __future__ import annotations

from pathlib import Path

import pytest
from services.config import clear_settings_cache


@pytest.fixture(autouse=True)
def reset_settings_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    clear_settings_cache()
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    yield
    clear_settings_cache()
