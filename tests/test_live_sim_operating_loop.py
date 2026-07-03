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
    monkeypatch.setattr(core_api, "load_settings", lambda: settings)
    monkeypatch.setattr(core_api, "market_is_weekday", lambda: True)
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
    monkeypatch.setattr(core_api, "load_settings", lambda: settings)
    monkeypatch.setattr(core_api, "market_is_weekday", lambda: True)
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


def test_live_sim_operating_cycle_uses_fresh_queue_command_setting(
    tmp_path,
    monkeypatch,
) -> None:
    startup_settings = Settings(
        trading_db_path=tmp_path / "startup.sqlite3",
        live_sim_operating_loop_queue_commands=False,
    )
    fresh_settings = Settings(
        trading_db_path=tmp_path / "fresh.sqlite3",
        live_sim_operating_loop_queue_commands=True,
    )
    captured: dict[str, Any] = {}
    connection = _FakeConnection()
    monkeypatch.setattr(core_api, "load_settings", lambda: fresh_settings)
    monkeypatch.setattr(core_api, "market_is_weekday", lambda: True)
    monkeypatch.setattr(core_api, "market_time_str", lambda: "10:00:00")
    monkeypatch.setattr(core_api, "initialize_database", lambda path: connection)

    def operating_cycle(*args: Any, **kwargs: Any) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(core_api, "run_live_sim_operating_cycle_once", operating_cycle)

    core_api._run_live_sim_operating_cycle_once(startup_settings)

    assert captured["args"] == (connection,)
    assert captured["kwargs"]["settings"] is fresh_settings
    assert captured["kwargs"]["mode"] is None
    assert captured["kwargs"]["queue_commands"] is True
    assert connection.closed is True


def test_live_sim_operating_cycle_reloads_settings_each_tick(
    tmp_path,
    monkeypatch,
) -> None:
    captured_settings: list[Settings] = []
    captured_queue_commands: list[bool] = []
    opened_paths: list[object] = []
    connections = [_FakeConnection(), _FakeConnection()]

    def initialize(path: object) -> _FakeConnection:
        opened_paths.append(path)
        return connections[len(opened_paths) - 1]

    def operating_cycle(*args: Any, **kwargs: Any) -> None:
        captured_settings.append(kwargs["settings"])
        captured_queue_commands.append(kwargs["queue_commands"])

    monkeypatch.setattr(core_api, "market_is_weekday", lambda: True)
    monkeypatch.setattr(core_api, "market_time_str", lambda: "10:00:00")
    monkeypatch.setattr(core_api, "initialize_database", initialize)
    monkeypatch.setattr(core_api, "run_live_sim_operating_cycle_once", operating_cycle)

    startup_settings = Settings(trading_db_path=tmp_path / "startup.sqlite3")
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "first.sqlite3"))
    monkeypatch.setenv("LIVE_SIM_KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS", "true")
    core_api._run_live_sim_operating_cycle_once(startup_settings)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "second.sqlite3"))
    monkeypatch.setenv("LIVE_SIM_KILL_SWITCH", "true")
    monkeypatch.setenv("LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS", "false")
    core_api._run_live_sim_operating_cycle_once(startup_settings)

    assert [settings.live_sim_kill_switch for settings in captured_settings] == [False, True]
    assert captured_queue_commands == [True, False]
    assert opened_paths == [tmp_path / "first.sqlite3", tmp_path / "second.sqlite3"]
    assert all(connection.closed for connection in connections)


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
