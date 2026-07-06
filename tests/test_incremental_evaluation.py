from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import services.runtime.incremental_evaluation as incremental_evaluation
from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_price_tick_event
from services.config import Settings
from services.runtime.incremental_evaluation import (
    enqueue_incremental_evaluation_for_event,
    get_incremental_evaluation_status,
    process_incremental_evaluation_batch,
)
from storage.sqlite import initialize_database, open_connection
from tests.test_strategy_service import _insert_strategy_fixture, _settings


def test_price_tick_dirty_set_evaluates_strategy_and_risk(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental.sqlite3")
    settings = _incremental_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _insert_active_theme_source(connection, candidate_id=candidate_id)

    enqueue = enqueue_incremental_evaluation_for_event(
        connection,
        make_price_tick_event(price=97_500, volume=1_100),
        settings=settings,
    )
    status_before = get_incremental_evaluation_status(connection, settings=settings)
    result = process_incremental_evaluation_batch(connection, settings=settings)
    status_after = get_incremental_evaluation_status(connection, settings=settings)

    strategy_count = connection.execute(
        "SELECT COUNT(*) AS count FROM strategy_observations_latest"
    ).fetchone()["count"]
    risk_count = connection.execute(
        "SELECT COUNT(*) AS count FROM risk_observations_latest"
    ).fetchone()["count"]
    connection.close()

    assert enqueue.status == "ENQUEUED"
    assert enqueue.enqueued_count == 1
    assert enqueue.candidate_ids == (candidate_id,)
    assert status_before["queued_count"] == 1
    assert result.status == "COMPLETED"
    assert result.processed_count == 1
    assert result.strategy_observation_count == 1
    assert result.risk_observation_count == 1
    assert status_after["queued_count"] == 0
    assert strategy_count == 1
    assert risk_count == 1


def test_incremental_evaluation_releases_lock_between_chunks(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental-chunks.sqlite3")
    settings = _incremental_settings(
        incremental_evaluation_batch_size=6,
        strategy_engine_enabled=False,
        risk_gate_enabled=False,
    )
    for index in range(6):
        code = f"{index + 1:06d}"
        candidate_id = _insert_strategy_fixture(
            connection,
            candidate_id=f"CAND-2026-06-27-{code}-1",
            code=code,
            name=f"종목{index + 1}",
        )
        _insert_incremental_queue_row(connection, candidate_id=candidate_id, code=code)

    active_locks: list[bool] = []
    lock_chunk_limits: list[int] = []
    sleeps: list[float] = []

    @contextmanager
    def recording_lock(*args, **kwargs):
        active_locks.append(True)
        lock_chunk_limits.append(kwargs["details"]["chunk_limit"])
        try:
            yield "test-lock"
        finally:
            active_locks.pop()

    def sleep(seconds: float) -> None:
        assert active_locks == []
        sleeps.append(seconds)

    monkeypatch.setattr(incremental_evaluation, "runtime_execution_lock", recording_lock)
    monkeypatch.setattr(incremental_evaluation.time, "sleep", sleep)
    monkeypatch.setattr(
        incremental_evaluation,
        "refresh_candidate_context",
        lambda *args, **kwargs: SimpleNamespace(error_count=0),
    )

    result = process_incremental_evaluation_batch(connection, settings=settings)
    queued_after = get_incremental_evaluation_status(connection, settings=settings)[
        "queued_count"
    ]
    connection.close()

    assert result.status == "COMPLETED"
    assert result.polled_count == 6
    assert result.processed_count == 6
    assert queued_after == 0
    assert lock_chunk_limits == [5, 1]
    assert sleeps == [0.0]


def test_price_tick_dirty_set_ignores_codes_without_active_candidate(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental-empty.sqlite3")
    result = enqueue_incremental_evaluation_for_event(
        connection,
        make_price_tick_event(code="005930"),
        settings=_incremental_settings(),
    )
    queued_count = get_incremental_evaluation_status(
        connection,
        settings=_incremental_settings(),
    )["queued_count"]
    connection.close()

    assert result.status == "IGNORED_NO_ACTIVE_CANDIDATE"
    assert result.enqueued_count == 0
    assert queued_count == 0


def test_gateway_enqueues_and_operator_run_once_processes_incremental_evaluation(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "incremental-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    monkeypatch.setenv("MARKET_DATA_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_SOURCE_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_EPISODE_TTL_SEC", "999999999")
    monkeypatch.setenv("STRATEGY_ENGINE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STALE_TICK_SEC", "999999999")

    connection = initialize_database(db_path)
    candidate_id = _insert_strategy_fixture(connection)
    _insert_active_theme_source(connection, candidate_id=candidate_id)
    connection.close()

    with TestClient(app) as client:
        headers = {"X-Local-Token": "test-token"}
        tick_response = client.post(
            "/api/gateway/events",
            json=make_price_tick_event(price=98_000, volume=1_200).to_dict(),
            headers=headers,
        )
        status_before = client.get("/api/operator/incremental-evaluation/status")
        run_once = client.post(
            "/api/operator/incremental-evaluation/run-once",
            headers=headers,
        )
        status_after = client.get("/api/operator/incremental-evaluation/status")

    connection = open_connection(db_path)
    strategy_count = connection.execute(
        "SELECT COUNT(*) AS count FROM strategy_observations_latest"
    ).fetchone()["count"]
    risk_count = connection.execute(
        "SELECT COUNT(*) AS count FROM risk_observations_latest"
    ).fetchone()["count"]
    connection.close()

    assert tick_response.status_code == 200
    assert tick_response.json()["projection_statuses"]["incremental_evaluation"] == "ENQUEUED"
    assert status_before.status_code == 200
    assert status_before.json()["queued_count"] == 1
    assert run_once.status_code == 200
    assert run_once.json()["processed_count"] == 1
    assert run_once.json()["no_order_side_effects"] is True
    assert status_after.json()["queued_count"] == 0
    assert strategy_count == 1
    assert risk_count == 1


def _incremental_settings(**overrides) -> Settings:
    values = {
        "risk_gate_stale_tick_sec": 999_999_999,
        "risk_gate_strategy_stale_sec": 999_999_999,
    }
    values.update(overrides)
    return _settings(**values)


def _insert_active_theme_source(
    connection,
    *,
    candidate_id: str,
    trade_date: str = "2026-06-27",
    code: str = "005930",
    name: str = "삼성전자",
) -> None:
    now = datetime_to_wire(utc_now())
    payload = {
        "theme_id": f"theme-{code}",
        "theme_name": "반도체",
        "theme_state": "LEADING",
        "member_role": "LEADER_CANDIDATE",
        "stock_role": "LEADER_CANDIDATE",
        "source_detail": {
            "fresh_coverage_ratio": 1.0,
            "rising_ratio": 0.6,
        },
        "reason_codes": ["TEST_THEME_SOURCE"],
        "observe_only": True,
        "not_order_signal": True,
    }
    connection.execute(
        """
        INSERT INTO candidate_sources_latest (
            trade_date,
            code,
            source_type,
            source_id,
            candidate_instance_id,
            name,
            active,
            first_seen_at,
            last_seen_at,
            last_event_id,
            payload_json
        )
        VALUES (?, ?, 'THEME_LEADER', ?, ?, ?, 1, ?, ?, ?, ?)
        """,
        (
            trade_date,
            code,
            f"theme-{code}",
            candidate_id,
            name,
            now,
            now,
            f"source-{code}",
            json.dumps(payload, ensure_ascii=False),
        ),
    )
    connection.commit()


def _insert_incremental_queue_row(
    connection,
    *,
    candidate_id: str,
    code: str,
    trade_date: str = "2026-06-27",
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO incremental_evaluation_queue (
            candidate_instance_id,
            trade_date,
            code,
            reason,
            source_event_id,
            priority,
            enqueued_at,
            updated_at,
            attempts,
            last_error
        )
        VALUES (?, ?, ?, 'PRICE_TICK', ?, 100, ?, ?, 0, NULL)
        """,
        (candidate_id, trade_date, code, f"evt-{code}", now, now),
    )
    connection.commit()
