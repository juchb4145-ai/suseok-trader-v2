from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import services.runtime.incremental_evaluation as incremental
from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from services.config import Settings
from services.runtime.incremental_evaluation import (
    enqueue_incremental_evaluation_for_code,
    get_incremental_evaluation_status,
    list_incremental_evaluation_dead_letters,
    process_incremental_evaluation_batch,
    reset_incremental_evaluation_dead_letter,
    sweep_incremental_evaluation_retry_exhausted,
)
from storage.sqlite import SCHEMA_VERSION, initialize_database, open_connection
from tests.test_strategy_service import _insert_strategy_fixture


def _settings(**overrides) -> Settings:
    values = {
        "incremental_evaluation_enabled": True,
        "incremental_evaluation_worker_enabled": False,
        "incremental_evaluation_retry_limit": 2,
        "incremental_evaluation_backlog_warn_count": 10,
        "incremental_evaluation_backlog_fail_count": 100,
        "incremental_evaluation_stale_warn_sec": 60,
        "incremental_evaluation_stale_fail_sec": 300,
        "strategy_engine_enabled": False,
        "risk_gate_enabled": False,
    }
    values.update(overrides)
    return replace(Settings(), **values)


def _insert_queue_row(
    connection,
    *,
    candidate_id: str,
    code: str,
    attempts: int = 0,
    updated_at: str | None = None,
) -> None:
    now = updated_at or datetime_to_wire(utc_now())
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
        VALUES (?, '2026-07-10', ?, 'PRICE_TICK', ?, 100, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            code,
            f"evt-{candidate_id}",
            now,
            now,
            attempts,
            "forced retry exhaustion" if attempts else None,
        ),
    )
    connection.commit()


def test_schema_58_dead_letter_migration_is_reentrant(tmp_path) -> None:
    db_path = tmp_path / "incremental-dead-letter-migration.sqlite3"
    connection = initialize_database(db_path)
    connection.execute("UPDATE app_metadata SET value = '57' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    rerun = initialize_database(db_path)
    try:
        schema_version = migrated.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()["value"]
        columns = {
            row["name"]
            for row in migrated.execute(
                "PRAGMA table_info(incremental_evaluation_dead_letters)"
            ).fetchall()
        }
        indexes = {
            row["name"]
            for row in rerun.execute(
                "PRAGMA index_list(incremental_evaluation_dead_letters)"
            ).fetchall()
        }
        disposition_indexes = {
            row["name"]
            for row in rerun.execute(
                "PRAGMA index_list(incremental_evaluation_dead_letter_dispositions)"
            ).fetchall()
        }
        tables = {
            row["name"]
            for row in rerun.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        triggers = {
            row["name"]
            for row in rerun.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
        }
    finally:
        migrated.close()
        rerun.close()

    assert schema_version == str(SCHEMA_VERSION) == "61"
    assert {"dead_letter_id", "candidate_instance_id", "attempts", "status"} <= columns
    assert "uq_incremental_evaluation_dead_letter_active" not in indexes
    assert "idx_incremental_evaluation_dead_letter_candidate_time" in indexes
    assert "incremental_evaluation_dead_letter_dispositions" in tables
    assert {
        "idx_incremental_dead_letter_disposition_effective",
        "idx_incremental_dead_letter_disposition_session",
    } <= disposition_indexes
    assert {
        "trg_incremental_evaluation_dead_letters_no_update",
        "trg_incremental_evaluation_dead_letters_no_delete",
        "trg_incremental_dead_letter_dispositions_no_update",
        "trg_incremental_dead_letter_dispositions_no_delete",
    } <= triggers


def test_incremental_status_reports_backlog_age_and_stale_severity(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental-health.sqlite3")
    old = datetime_to_wire(utc_now() - timedelta(seconds=120))
    _insert_queue_row(
        connection,
        candidate_id="candidate-old",
        code="005930",
        updated_at=old,
    )
    _insert_queue_row(connection, candidate_id="candidate-new", code="000660")
    settings = _settings(
        incremental_evaluation_backlog_warn_count=2,
        incremental_evaluation_backlog_fail_count=3,
    )

    status = get_incremental_evaluation_status(connection, settings=settings)
    connection.close()

    assert status["status"] == "WARN"
    assert status["queued_count"] == 2
    assert status["stale_queue_count"] == 1
    assert status["stale_fail_count"] == 0
    assert status["oldest_age_sec"] >= 119
    assert status["oldest_updated_age_sec"] >= 119
    assert status["reason_codes"] == [
        "INCREMENTAL_QUEUE_BACKLOG_WARN",
        "INCREMENTAL_QUEUE_STALE_WARN",
    ]


def test_retry_limit_moves_failed_candidate_to_dead_letter_atomically(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "incremental-failure.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)
    _insert_queue_row(connection, candidate_id=candidate_id, code="005930")
    settings = _settings()

    def fail_refresh(*args, **kwargs):
        raise RuntimeError("forced incremental failure")

    monkeypatch.setattr(incremental, "refresh_candidate_context", fail_refresh)
    first = process_incremental_evaluation_batch(connection, settings=settings, limit=1)
    after_first = connection.execute(
        "SELECT attempts, last_error FROM incremental_evaluation_queue"
    ).fetchone()
    second = process_incremental_evaluation_batch(connection, settings=settings, limit=1)
    queue_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()["count"]
    dead_letters = list_incremental_evaluation_dead_letters(connection)
    status = get_incremental_evaluation_status(connection, settings=settings)
    connection.close()

    assert first.error_count == 1
    assert first.dead_letter_count == 0
    assert after_first["attempts"] == 1
    assert "forced incremental failure" in after_first["last_error"]
    assert second.error_count == 1
    assert second.dead_letter_count == 1
    assert queue_count == 0
    assert len(dead_letters) == 1
    assert dead_letters[0]["candidate_instance_id"] == candidate_id
    assert dead_letters[0]["attempts"] == 2
    assert status["status"] == "FAIL"
    assert status["dead_letter_count"] == 1


def test_legacy_sweep_blocks_new_event_and_preserves_failure_evidence(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental-legacy.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)
    _insert_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
        attempts=3,
    )
    settings = _settings(incremental_evaluation_retry_limit=3)

    batch = process_incremental_evaluation_batch(connection, settings=settings, limit=1)
    first_dead_letter = list_incremental_evaluation_dead_letters(connection)[0]
    enqueue = enqueue_incremental_evaluation_for_code(
        connection,
        "005930",
        source_event_id="evt-new-price",
        event_id="evt-new-price",
        settings=settings,
    )
    queue_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()["count"]
    preserved = connection.execute(
        """
        SELECT status, attempts, last_error
        FROM incremental_evaluation_dead_letters
        WHERE dead_letter_id = ?
        """,
        (first_dead_letter["dead_letter_id"],),
    ).fetchone()
    connection.close()

    assert batch.status == "COMPLETED_WITH_DEAD_LETTERS"
    assert batch.legacy_retry_exhausted_dead_lettered_count == 1
    assert enqueue.status == "BLOCKED_DEAD_LETTER"
    assert enqueue.retry_exhausted_dead_lettered_count == 0
    assert enqueue.dead_letter_blocked_count == 1
    assert enqueue.enqueued_count == 0
    assert queue_count == 0
    assert preserved["status"] == "DEAD_LETTER"
    assert preserved["attempts"] == 3
    assert preserved["last_error"] == "forced retry exhaustion"


def test_new_event_moves_unswept_exhausted_row_then_blocks_enqueue(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental-new-event.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)
    _insert_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
        attempts=2,
    )
    settings = _settings()

    enqueue = enqueue_incremental_evaluation_for_code(
        connection,
        "005930",
        source_event_id="evt-recovery",
        event_id="evt-recovery",
        settings=settings,
    )
    queue_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()["count"]
    dead_letter_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_dead_letters"
    ).fetchone()["count"]
    connection.close()

    assert enqueue.retry_exhausted_dead_lettered_count == 1
    assert enqueue.status == "BLOCKED_DEAD_LETTER"
    assert enqueue.dead_letter_blocked_count == 1
    assert queue_count == 0
    assert dead_letter_count == 1


def test_legacy_retry_reset_is_disabled_without_guarded_recovery(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental-reset.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)
    _insert_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
        attempts=2,
    )
    settings = _settings()
    assert sweep_incremental_evaluation_retry_exhausted(
        connection,
        settings=settings,
    ) == 1
    dead_letter_id = list_incremental_evaluation_dead_letters(connection)[0][
        "dead_letter_id"
    ]

    reset = reset_incremental_evaluation_dead_letter(
        connection,
        dead_letter_id,
        reset_by="test_operator",
    )
    queue = connection.execute(
        "SELECT attempts, last_error FROM incremental_evaluation_queue"
    ).fetchone()
    dead_letter = connection.execute(
        """
        SELECT status, reset_by, reset_evidence_json
        FROM incremental_evaluation_dead_letters
        WHERE dead_letter_id = ?
        """,
        (dead_letter_id,),
    ).fetchone()
    connection.close()

    assert reset["status"] == "UNGUARDED_RESET_DISABLED"
    assert reset["reset_count"] == 0
    assert queue is None
    assert dead_letter["status"] == "DEAD_LETTER"
    assert dead_letter["reset_by"] is None
    assert dead_letter["reset_evidence_json"] == "{}"


def test_operator_exposes_effective_dead_letter_and_rejects_unguarded_reset(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "incremental-operations-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    connection = initialize_database(db_path)
    candidate_id = _insert_strategy_fixture(connection)
    _insert_queue_row(
        connection,
        candidate_id=candidate_id,
        code="005930",
        attempts=3,
    )
    settings = _settings(incremental_evaluation_retry_limit=3)
    sweep_incremental_evaluation_retry_exhausted(connection, settings=settings)
    dead_letter_id = list_incremental_evaluation_dead_letters(connection)[0][
        "dead_letter_id"
    ]
    connection.close()

    with TestClient(app) as client:
        status_before = client.get("/api/operator/incremental-evaluation/status")
        dead_letters = client.get(
            "/api/operator/incremental-evaluation/dead-letters?limit=10"
        )
        effective = client.get(
            "/api/operator/incremental-evaluation/dead-letters/effective?limit=10"
        )
        preview = client.get(
            "/api/operator/incremental-evaluation/dead-letters/disposition-preview",
            params={"dead_letter_id": dead_letter_id},
        )
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections=incremental_evaluation"
        )
        operator = client.get("/api/operator/status")
        reset = client.post(
            "/api/operator/incremental-evaluation/dead-letters/reset",
            params={"dead_letter_id": dead_letter_id},
            headers={"X-Local-Token": "test-token"},
        )
        status_after = client.get("/api/operator/incremental-evaluation/status")

    connection = open_connection(db_path)
    try:
        command_count = connection.execute(
            "SELECT COUNT(*) AS count FROM gateway_commands"
        ).fetchone()["count"]
    finally:
        connection.close()

    assert status_before.json()["status"] == "FAIL"
    assert status_before.json()["dead_letter_count"] == 1
    assert status_before.json()["raw_dead_letter_count"] == 1
    assert status_before.json()["effective_dead_letter_count"] == 1
    assert status_before.json()["active_unresolved_dead_letter_count"] == 1
    assert status_before.json()["historical_pending_disposition_count"] == 0
    assert status_before.json()["historical_disposed_dead_letter_count"] == 0
    assert status_before.json()["manual_review_dead_letter_count"] == 0
    assert status_before.json()["fast_0_status"] == "BLOCKED"
    assert dead_letters.json()["count"] == 1
    assert dead_letters.json()["read_only"] is True
    assert effective.status_code == 200
    assert effective.json()["count"] == 1
    assert effective.json()["effective_status"]["raw_dead_letter_count"] == 1
    assert (
        effective.json()["effective_status"]["effective_dead_letter_count"] == 1
    )
    assert preview.status_code == 409
    assert preview.json()["detail"]["eligible"] is False
    assert preview.json()["detail"]["read_only"] is True
    assert dashboard.json()["incremental_evaluation"]["dead_letter_count"] == 1
    assert operator.json()["incremental_evaluation"]["dead_letter_count"] == 1
    assert reset.status_code == 409
    assert reset.json()["detail"]["status"] == "UNGUARDED_RESET_DISABLED"
    assert status_after.json()["dead_letter_count"] == 1
    assert status_after.json()["queued_count"] == 0
    assert command_count == 0
