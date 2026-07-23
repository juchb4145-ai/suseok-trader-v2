from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import services.runtime.incremental_evaluation as incremental_evaluation
from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_price_tick_event
from services.config import Settings
from services.runtime.incremental_evaluation import (
    enqueue_incremental_evaluation_for_event,
    enqueue_incremental_evaluation_for_fresh_candidates,
    get_incremental_evaluation_status,
    process_incremental_evaluation_batch,
)
from storage.sqlite import initialize_database, open_connection
from tests.test_strategy_service import _insert_strategy_fixture, _settings


def test_price_tick_dirty_set_evaluates_strategy_and_risk(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental.sqlite3")
    settings = _incremental_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _insert_active_theme_source(connection, candidate_id=candidate_id)
    required_row_calls: list[tuple[str, str, str]] = []
    real_required_row = incremental_evaluation._required_pipeline_row

    def record_required_row(
        target_connection,
        *,
        table_name,
        id_column,
        row_id,
    ):
        required_row_calls.append((table_name, id_column, row_id))
        return real_required_row(
            target_connection,
            table_name=table_name,
            id_column=id_column,
            row_id=row_id,
        )

    monkeypatch.setattr(
        incremental_evaluation,
        "_required_pipeline_row",
        record_required_row,
    )

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
    entry_count = connection.execute(
        "SELECT COUNT(*) AS count FROM entry_timing_evaluations"
    ).fetchone()["count"]
    plan_count = connection.execute(
        "SELECT COUNT(*) AS count FROM order_plan_drafts"
    ).fetchone()["count"]
    pipeline_rows = {
        "strategy": connection.execute(
            "SELECT * FROM strategy_observations_latest"
        ).fetchone(),
        "risk": connection.execute(
            "SELECT * FROM risk_observations_latest"
        ).fetchone(),
        "entry": connection.execute(
            "SELECT * FROM entry_timing_evaluations"
        ).fetchone(),
        "plan": connection.execute(
            "SELECT * FROM order_plan_drafts"
        ).fetchone(),
    }
    connection.close()

    assert enqueue.status == "ENQUEUED"
    assert enqueue.enqueued_count == 1
    assert enqueue.candidate_ids == (candidate_id,)
    assert status_before["queued_count"] == 1
    assert result.status == "COMPLETED"
    assert result.processed_count == 1
    assert result.strategy_observation_count == 1
    assert result.risk_observation_count == 1
    assert result.entry_timing_evaluation_count == 1
    assert result.order_plan_draft_count == 1
    assert result.plan_ready_count == 0
    assert status_after["queued_count"] == 0
    assert strategy_count == 1
    assert risk_count == 1
    assert entry_count == 1
    assert plan_count == 1
    assert {row["source_run_id"] for row in pipeline_rows.values()} == {
        result.run_id
    }
    assert len({row["source_watermark_hash"] for row in pipeline_rows.values()}) == 1
    assert (
        pipeline_rows["risk"]["strategy_observation_id"]
        == pipeline_rows["strategy"]["strategy_observation_id"]
    )
    assert (
        pipeline_rows["entry"]["strategy_observation_id"]
        == pipeline_rows["strategy"]["strategy_observation_id"]
    )
    assert (
        pipeline_rows["entry"]["risk_observation_id"]
        == pipeline_rows["risk"]["risk_observation_id"]
    )
    assert (
        pipeline_rows["plan"]["entry_timing_evaluation_id"]
        == pipeline_rows["entry"]["entry_timing_evaluation_id"]
    )
    assert (
        "order_plan_drafts_latest",
        "idempotency_key",
        pipeline_rows["plan"]["idempotency_key"],
    ) in required_row_calls


def test_incremental_evaluation_releases_lock_between_chunks(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental-chunks.sqlite3")
    settings = _incremental_settings(
        incremental_evaluation_batch_size=6,
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
    monkeypatch.setattr(
        incremental_evaluation,
        "evaluate_candidate_strategy",
        lambda _connection, candidate_id, **_kwargs: SimpleNamespace(
            candidate_instance_id=candidate_id,
            trade_date="2026-06-27",
            strategy_observation_id=f"strategy-{candidate_id}",
            observe_only=True,
        ),
    )
    monkeypatch.setattr(
        incremental_evaluation,
        "save_strategy_observation",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        incremental_evaluation,
        "evaluate_risk_for_strategy_observation",
        lambda _connection, strategy_id, **_kwargs: SimpleNamespace(
            risk_observation_id=f"risk-{strategy_id}",
        ),
    )
    monkeypatch.setattr(
        incremental_evaluation,
        "save_risk_observation",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        incremental_evaluation,
        "evaluate_entry_timing",
        lambda *args, **kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        incremental_evaluation,
        "_assert_incremental_pipeline_complete",
        lambda *args, **kwargs: {
            "entry_timing_evaluation_count": 1,
            "order_plan_draft_count": 0,
            "plan_ready_count": 0,
        },
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


@pytest.mark.parametrize(
    "disabled_stage",
    (
        "strategy_engine_enabled",
        "risk_gate_enabled",
        "entry_timing_enabled",
    ),
)
def test_incremental_required_stage_disabled_preserves_queue(
    tmp_path,
    disabled_stage,
) -> None:
    connection = initialize_database(
        tmp_path / f"incremental-stage-disabled-{disabled_stage}.sqlite3"
    )
    candidate_id = _insert_strategy_fixture(connection)
    _insert_incremental_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
    )

    result = process_incremental_evaluation_batch(
        connection,
        settings=_incremental_settings(**{disabled_stage: False}),
        limit=1,
    )
    queue = connection.execute(
        "SELECT attempts, last_error FROM incremental_evaluation_queue"
    ).fetchone()
    pipeline_count = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM strategy_observations)
            + (SELECT COUNT(*) FROM risk_observations)
            + (SELECT COUNT(*) FROM entry_timing_evaluations)
            + (SELECT COUNT(*) FROM order_plan_drafts)
            AS count
        """
    ).fetchone()["count"]
    connection.close()

    assert result.status == "DISABLED_REQUIRED_STAGE"
    assert result.queued_before == 1
    assert result.queued_after == 1
    assert result.polled_count == 0
    assert result.processed_count == 0
    assert queue["attempts"] == 0
    assert queue["last_error"] is None
    assert pipeline_count == 0


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


def test_backfill_enqueues_fresh_candidate_without_latest_evaluation(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental-backfill.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)

    result = enqueue_incremental_evaluation_for_fresh_candidates(
        connection,
        trade_date="2026-06-27",
        settings=_incremental_settings(entry_timing_stale_max_seconds=999_999_999),
    )
    row = connection.execute("SELECT * FROM incremental_evaluation_queue").fetchone()
    connection.close()

    assert result.status == "ENQUEUED"
    assert result.enqueued_count == 1
    assert row["candidate_instance_id"] == candidate_id
    assert row["reason"] == "CANDIDATE_EVALUATION_BACKFILL"


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


def test_gateway_candidate_quote_refresh_tr_response_enqueues_incremental_evaluation(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "incremental-candidate-quote.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")

    connection = initialize_database(db_path)
    candidate_id = _insert_strategy_fixture(connection)
    connection.close()
    response = BrokerTrResponse(
        request_id="candidate_quote_refresh:2026-06-27:005930:1",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        success=True,
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "현재가": "+98000",
                "등락율": "+1.5",
                "거래량": "1200",
                "거래대금": "117600000",
                "고가": "+99000",
                "저가": "-96000",
            }
        ],
    )
    event = GatewayEvent(
        event_id="evt_candidate_quote_refresh_incremental",
        event_type="tr_response",
        source="test-gateway",
        payload=response.to_dict(),
    )

    with TestClient(app) as client:
        result = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    queue = connection.execute("SELECT * FROM incremental_evaluation_queue").fetchone()
    latest = connection.execute(
        "SELECT price FROM market_ticks_latest WHERE code = '005930' AND exchange = 'KRX'"
    ).fetchone()
    connection.close()

    assert result.status_code == 200
    assert result.json()["projection_statuses"]["incremental_evaluation"] == "ENQUEUED"
    assert queue["candidate_instance_id"] == candidate_id
    assert queue["reason"] == "CANDIDATE_QUOTE_REFRESH"
    assert latest["price"] == 98_000


def test_incremental_risk_failure_rolls_back_strategy_and_keeps_queue_error(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental-risk-rollback.sqlite3")
    settings = _incremental_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _insert_active_theme_source(connection, candidate_id=candidate_id)
    _insert_incremental_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
    )

    def fail_risk(*args, **kwargs):
        raise RuntimeError("forced exact risk failure")

    monkeypatch.setattr(
        incremental_evaluation,
        "evaluate_risk_for_strategy_observation",
        fail_risk,
    )
    result = process_incremental_evaluation_batch(
        connection,
        settings=settings,
        limit=1,
    )
    queue = connection.execute(
        "SELECT attempts, last_error FROM incremental_evaluation_queue"
    ).fetchone()
    counts = {
        table_name: connection.execute(
            f"SELECT COUNT(*) AS count FROM {table_name}"
        ).fetchone()["count"]
        for table_name in (
            "strategy_observations",
            "risk_observations",
            "entry_timing_evaluations",
            "order_plan_drafts",
        )
    }
    connection.close()

    assert result.status == "COMPLETED_WITH_ERRORS"
    assert result.error_count == 1
    assert result.processed_count == 0
    assert result.strategy_observation_count == 0
    assert result.risk_observation_count == 0
    assert result.entry_timing_evaluation_count == 0
    assert result.order_plan_draft_count == 0
    assert queue["attempts"] == 1
    assert "forced exact risk failure" in queue["last_error"]
    assert set(counts.values()) == {0}


def test_incremental_queue_error_rolls_back_when_fence_check_fails(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental-error-fence.sqlite3")
    settings = _incremental_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _insert_active_theme_source(connection, candidate_id=candidate_id)
    _insert_incremental_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
    )
    real_assert_fence = incremental_evaluation.assert_runtime_execution_fence
    fence_call_count = 0

    def fail_after_queue_error_write(target_connection):
        nonlocal fence_call_count
        fence_call_count += 1
        if fence_call_count == 4:
            raise RuntimeError("forced queue-error fence failure")
        return real_assert_fence(target_connection)

    monkeypatch.setattr(
        incremental_evaluation,
        "evaluate_risk_for_strategy_observation",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("forced pipeline failure")
        ),
    )
    monkeypatch.setattr(
        incremental_evaluation,
        "assert_runtime_execution_fence",
        fail_after_queue_error_write,
    )

    with pytest.raises(RuntimeError, match="forced queue-error fence failure"):
        process_incremental_evaluation_batch(
            connection,
            settings=settings,
            limit=1,
        )

    queue = connection.execute(
        "SELECT attempts, last_error FROM incremental_evaluation_queue"
    ).fetchone()
    pipeline_count = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM strategy_observations)
            + (SELECT COUNT(*) FROM risk_observations)
            + (SELECT COUNT(*) FROM entry_timing_evaluations)
            + (SELECT COUNT(*) FROM order_plan_drafts)
            AS count
        """
    ).fetchone()["count"]
    lock_count = connection.execute(
        "SELECT COUNT(*) AS count FROM runtime_execution_locks"
    ).fetchone()["count"]
    in_transaction = connection.in_transaction
    connection.close()

    assert fence_call_count == 4
    assert queue["attempts"] == 0
    assert queue["last_error"] is None
    assert pipeline_count == 0
    assert lock_count == 0
    assert in_transaction is False


def test_incremental_entry_failure_after_writes_rolls_back_whole_pipeline(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental-entry-rollback.sqlite3")
    settings = _incremental_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _insert_active_theme_source(connection, candidate_id=candidate_id)
    _insert_incremental_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
    )
    real_evaluate_entry_timing = incremental_evaluation.evaluate_entry_timing

    def evaluate_then_fail(*args, **kwargs):
        real_evaluate_entry_timing(*args, **kwargs)
        raise RuntimeError("forced post-entry failure")

    monkeypatch.setattr(
        incremental_evaluation,
        "evaluate_entry_timing",
        evaluate_then_fail,
    )
    result = process_incremental_evaluation_batch(
        connection,
        settings=settings,
        limit=1,
    )
    queue = connection.execute(
        "SELECT attempts, last_error FROM incremental_evaluation_queue"
    ).fetchone()
    counts = {
        table_name: connection.execute(
            f"SELECT COUNT(*) AS count FROM {table_name}"
        ).fetchone()["count"]
        for table_name in (
            "strategy_observations",
            "risk_observations",
            "entry_timing_evaluations",
            "order_plan_drafts",
        )
    }
    connection.close()

    assert result.error_count == 1
    assert result.processed_count == 0
    assert queue["attempts"] == 1
    assert "forced post-entry failure" in queue["last_error"]
    assert set(counts.values()) == {0}


def test_incremental_rejects_entry_fallback_to_another_source_run(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental-source-run-rollback.sqlite3")
    settings = _incremental_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _insert_active_theme_source(connection, candidate_id=candidate_id)
    old_strategy = incremental_evaluation.evaluate_candidate_strategy(
        connection,
        candidate_id,
        settings=settings,
    )
    incremental_evaluation.save_strategy_observation(
        connection,
        old_strategy,
        source_run_id="older-source-run",
    )
    old_risk = incremental_evaluation.evaluate_risk_for_strategy_observation(
        connection,
        old_strategy.strategy_observation_id,
        settings=settings,
    )
    incremental_evaluation.save_risk_observation(
        connection,
        old_risk,
        source_run_id="older-source-run",
    )
    connection.commit()
    _insert_incremental_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
    )

    monkeypatch.setattr(
        incremental_evaluation,
        "save_risk_observation",
        lambda *args, **kwargs: None,
    )
    result = process_incremental_evaluation_batch(
        connection,
        settings=settings,
        limit=1,
    )
    queue = connection.execute(
        "SELECT attempts, last_error FROM incremental_evaluation_queue"
    ).fetchone()
    latest_strategy = connection.execute(
        "SELECT * FROM strategy_observations_latest"
    ).fetchone()
    latest_risk = connection.execute(
        "SELECT * FROM risk_observations_latest"
    ).fetchone()
    counts = {
        table_name: connection.execute(
            f"SELECT COUNT(*) AS count FROM {table_name}"
        ).fetchone()["count"]
        for table_name in (
            "strategy_observations",
            "risk_observations",
            "entry_timing_evaluations",
            "order_plan_drafts",
        )
    }
    connection.close()

    assert result.error_count == 1
    assert result.processed_count == 0
    assert queue["attempts"] == 1
    assert "incremental entry timing" in queue["last_error"]
    assert counts == {
        "strategy_observations": 1,
        "risk_observations": 1,
        "entry_timing_evaluations": 0,
        "order_plan_drafts": 0,
    }
    assert latest_strategy["source_run_id"] == "older-source-run"
    assert latest_risk["source_run_id"] == "older-source-run"


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
