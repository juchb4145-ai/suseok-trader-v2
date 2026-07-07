from __future__ import annotations

from datetime import timedelta

import pytest
from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.runtime.evaluation_run_guard import (
    DEFAULT_EVALUATION_LOCK_TTL_SEC,
    EVALUATION_PIPELINE_LOCK,
    EvaluationRunLockError,
    _begin_immediate_with_retry,
    immediate_transaction,
)
from services.strategy_engine import evaluate_candidates
from storage.gateway_command_store import canonical_json
from storage.sqlite import initialize_database


def test_default_evaluation_lock_ttl_is_two_minutes() -> None:
    assert DEFAULT_EVALUATION_LOCK_TTL_SEC == 120


def test_active_evaluation_lock_blocks_strategy_run_before_run_row(tmp_path) -> None:
    connection = initialize_database(tmp_path / "evaluation-lock.sqlite3")
    _insert_lock(connection, owner_id="other-run", expires_in_sec=300)

    with pytest.raises(EvaluationRunLockError) as exc_info:
        evaluate_candidates(connection, settings=Settings())

    run_count = connection.execute(
        "SELECT COUNT(*) AS count FROM strategy_evaluation_runs"
    ).fetchone()["count"]
    lock_count = connection.execute(
        "SELECT COUNT(*) AS count FROM runtime_execution_locks"
    ).fetchone()["count"]
    connection.close()

    assert exc_info.value.lock_name == EVALUATION_PIPELINE_LOCK
    assert exc_info.value.owner_id == "other-run"
    assert run_count == 0
    assert lock_count == 1


def test_expired_evaluation_lock_is_replaced_and_released(tmp_path) -> None:
    connection = initialize_database(tmp_path / "evaluation-expired-lock.sqlite3")
    _insert_lock(connection, owner_id="expired-run", expires_in_sec=-1)

    result = evaluate_candidates(connection, settings=Settings())

    run_count = connection.execute(
        "SELECT COUNT(*) AS count FROM strategy_evaluation_runs"
    ).fetchone()["count"]
    lock_count = connection.execute(
        "SELECT COUNT(*) AS count FROM runtime_execution_locks"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "COMPLETED"
    assert run_count == 1
    assert lock_count == 0


def test_immediate_transaction_rolls_back_unhandled_error(tmp_path) -> None:
    connection = initialize_database(tmp_path / "evaluation-transaction.sqlite3")

    with pytest.raises(RuntimeError):
        with immediate_transaction(connection):
            connection.execute(
                """
                INSERT INTO runtime_execution_locks (
                    lock_name,
                    owner_id,
                    acquired_at,
                    expires_at,
                    detail_json
                )
                VALUES ('rollback-probe', 'owner', ?, ?, '{}')
                """,
                (
                    datetime_to_wire(utc_now()),
                    datetime_to_wire(utc_now() + timedelta(seconds=60)),
                ),
            )
            raise RuntimeError("boom")

    row_count = connection.execute(
        "SELECT COUNT(*) AS count FROM runtime_execution_locks"
    ).fetchone()["count"]
    connection.close()

    assert row_count == 0


def test_begin_immediate_retries_transient_database_lock(monkeypatch) -> None:
    connection = _FlakyBeginConnection()
    monkeypatch.setattr("services.runtime.evaluation_run_guard.time.sleep", lambda _: None)

    _begin_immediate_with_retry(connection)  # type: ignore[arg-type]

    assert connection.begin_attempts == 2


class _FlakyBeginConnection:
    in_transaction = False

    def __init__(self) -> None:
        self.begin_attempts = 0

    def execute(self, sql: str, *args):
        if sql == "BEGIN IMMEDIATE":
            self.begin_attempts += 1
            if self.begin_attempts == 1:
                raise __import__("sqlite3").OperationalError("database is locked")
        return None


def _insert_lock(
    connection,
    *,
    owner_id: str,
    expires_in_sec: int,
) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO runtime_execution_locks (
            lock_name,
            owner_id,
            acquired_at,
            expires_at,
            detail_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            EVALUATION_PIPELINE_LOCK,
            owner_id,
            datetime_to_wire(now),
            datetime_to_wire(now + timedelta(seconds=expires_in_sec)),
            canonical_json({"test": True}),
        ),
    )
    connection.commit()
