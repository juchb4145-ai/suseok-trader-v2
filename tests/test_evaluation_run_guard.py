from __future__ import annotations

import time
from datetime import timedelta

import pytest
from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.runtime.evaluation_run_guard import (
    DEFAULT_EVALUATION_LOCK_TTL_SEC,
    EVALUATION_PIPELINE_LOCK,
    EvaluationRunFenceError,
    EvaluationRunLockError,
    _begin_immediate_with_retry,
    get_runtime_execution_lock_status,
    immediate_transaction,
    runtime_execution_lock,
)
from services.strategy_engine import evaluate_candidates
from storage.gateway_command_store import canonical_json
from storage.sqlite import initialize_database, open_connection


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


def test_runtime_execution_lock_heartbeat_renews_lease(tmp_path) -> None:
    connection = initialize_database(tmp_path / "evaluation-heartbeat.sqlite3")

    with runtime_execution_lock(
        connection,
        owner_id="heartbeat-owner",
        ttl_sec=1,
        heartbeat_interval_sec=0.1,
    ) as lease:
        initial_expires_at = lease.expires_at
        time.sleep(0.35)
        row = connection.execute(
            """
            SELECT heartbeat_at, expires_at, process_id, thread_id, fencing_token
            FROM runtime_execution_locks
            WHERE lock_name = ?
            """,
            (EVALUATION_PIPELINE_LOCK,),
        ).fetchone()

    connection.close()

    assert row["heartbeat_at"] > lease.acquired_at
    assert row["expires_at"] > initial_expires_at
    assert row["process_id"] > 0
    assert row["thread_id"] > 0
    assert row["fencing_token"] == lease.fencing_token


def test_stale_owner_fencing_token_blocks_write_after_takeover(tmp_path) -> None:
    db_path = tmp_path / "evaluation-fence.sqlite3"
    first = initialize_database(db_path)
    second = open_connection(db_path)
    first.execute("CREATE TABLE fence_probe (value TEXT NOT NULL)")
    first.commit()

    try:
        with pytest.raises(EvaluationRunFenceError) as exit_error:
            with runtime_execution_lock(
                first,
                owner_id="owner-one",
                ttl_sec=60,
                heartbeat_interval_sec=0,
            ) as first_lease:
                first.execute(
                    """
                    UPDATE runtime_execution_locks
                    SET expires_at = ?, process_id = 999999, thread_id = 0
                    WHERE lock_name = ?
                    """,
                    (
                        datetime_to_wire(utc_now() - timedelta(seconds=1)),
                        EVALUATION_PIPELINE_LOCK,
                    ),
                )
                first.commit()

                with runtime_execution_lock(
                    second,
                    owner_id="owner-two",
                    ttl_sec=60,
                    heartbeat_interval_sec=0,
                ) as second_lease:
                    assert second_lease.fencing_token > first_lease.fencing_token
                    with pytest.raises(EvaluationRunFenceError) as exc_info:
                        with immediate_transaction(first, lease=first_lease):
                            first.execute(
                                "INSERT INTO fence_probe (value) VALUES ('stale')"
                            )

                    row_count = second.execute(
                        "SELECT COUNT(*) AS count FROM fence_probe"
                    ).fetchone()["count"]

        assert exit_error.value.owner_id == "owner-one"
        assert exc_info.value.current_owner_id == "owner-two"
        assert row_count == 0
    finally:
        first.close()
        second.close()


def test_fencing_token_is_monotonic_across_release(tmp_path) -> None:
    connection = initialize_database(tmp_path / "evaluation-fence-sequence.sqlite3")

    with runtime_execution_lock(
        connection,
        owner_id="owner-one",
        heartbeat_interval_sec=0,
    ) as first_lease:
        first_token = first_lease.fencing_token
    with runtime_execution_lock(
        connection,
        owner_id="owner-two",
        heartbeat_interval_sec=0,
    ) as second_lease:
        second_token = second_lease.fencing_token
        status = get_runtime_execution_lock_status(connection)

    connection.close()

    assert second_token == first_token + 1
    assert status["status"] == "PASS"
    assert status["active_count"] == 1
    assert status["locks"][0]["fencing_token"] == second_token


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
