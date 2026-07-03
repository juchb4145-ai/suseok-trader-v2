from __future__ import annotations

import asyncio
import logging
from typing import Any

from apps import core_api
from services.config import Settings
from services.runtime.evaluation_run_guard import EvaluationRunLockError


def test_live_sim_operating_loop_disabled_does_not_create_task(monkeypatch) -> None:
    created_tasks: list[object] = []

    def create_task(coro: object) -> object:
        created_tasks.append(coro)
        return object()

    monkeypatch.setattr(core_api.asyncio, "create_task", create_task)

    task = core_api._maybe_create_live_sim_operating_cycle_task(
        Settings(live_sim_operating_loop_enabled=False)
    )

    assert task is None
    assert created_tasks == []


def test_live_sim_operating_cycle_skips_outside_market_time(tmp_path, monkeypatch) -> None:
    settings = Settings(trading_db_path=tmp_path / "outside-market.sqlite3")
    monkeypatch.setattr(core_api, "market_time_str", lambda: "08:59:59")

    def fail_initialize_database(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("database should not open outside market hours")

    def fail_operating_cycle(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("operating cycle should not run outside market hours")

    monkeypatch.setattr(core_api, "initialize_database", fail_initialize_database)
    monkeypatch.setattr(core_api, "run_live_sim_operating_cycle_once", fail_operating_cycle)

    core_api._run_live_sim_operating_cycle_once(settings)


def test_live_sim_operating_cycle_uses_default_mode_and_never_upgrades_queue(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        trading_db_path=tmp_path / "inside-market.sqlite3",
        live_sim_operating_default_mode="PILOT_FULL_LIFECYCLE",
    )
    captured: dict[str, Any] = {}
    connection = _FakeConnection()
    monkeypatch.setattr(core_api, "market_time_str", lambda: "10:00:00")
    monkeypatch.setattr(core_api, "initialize_database", lambda path: connection)

    def operating_cycle(*args: Any, **kwargs: Any) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(core_api, "run_live_sim_operating_cycle_once", operating_cycle)

    core_api._run_live_sim_operating_cycle_once(settings)

    assert captured["args"] == (connection,)
    assert captured["kwargs"]["settings"] is settings
    assert captured["kwargs"]["mode"] is None
    assert captured["kwargs"]["queue_commands"] is False
    assert connection.closed is True


def test_live_sim_operating_cycle_tick_skips_lock_conflict(
    tmp_path,
    monkeypatch,
    caplog,
) -> None:
    settings = Settings(trading_db_path=tmp_path / "lock-conflict.sqlite3")

    def locked(settings: Settings) -> None:
        raise EvaluationRunLockError(
            lock_name="evaluation_pipeline",
            owner_id="manual-run",
            expires_at="2026-07-03T00:00:00Z",
        )

    monkeypatch.setattr(core_api, "_run_live_sim_operating_cycle_once", locked)
    caplog.set_level(logging.DEBUG, logger=core_api.logger.name)

    asyncio.run(core_api._live_sim_operating_cycle_tick(settings))

    assert "evaluation lock is held" in caplog.text


class _FakeConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True
