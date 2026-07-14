from __future__ import annotations

from dataclasses import replace

from apps.core_api import app
from domain.broker.events import GatewayEvent
from fastapi.testclient import TestClient
from services.config import Settings
from services.runtime import live_sim_lifecycle_consumer as consumer
from storage.event_store import append_gateway_event
from storage.sqlite import SCHEMA_VERSION, initialize_database, open_connection


def _event(event_id: str, *, event_type: str = "command_started") -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type=event_type,
        source="lifecycle-consumer-test",
        payload={"status": "observed"},
    )


def _worker_settings(**overrides) -> Settings:
    return replace(
        Settings(),
        live_sim_lifecycle_consumer_enabled=True,
        live_sim_lifecycle_worker_enabled=True,
        live_sim_lifecycle_retry_delay_sec=0.0,
        **overrides,
    )


def test_lifecycle_inbox_migration_is_reentrant(tmp_path) -> None:
    db_path = tmp_path / "lifecycle-migration.sqlite3"
    connection = initialize_database(db_path)
    connection.execute("UPDATE app_metadata SET value = '55' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    second = initialize_database(db_path)
    try:
        schema_version = migrated.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()["value"]
        columns = {
            row["name"]
            for row in migrated.execute(
                "PRAGMA table_info(live_sim_lifecycle_inbox)"
            ).fetchall()
        }
        indexes = {
            row["name"]
            for row in second.execute(
                "PRAGMA index_list(live_sim_lifecycle_inbox)"
            ).fetchall()
        }
    finally:
        migrated.close()
        second.close()

    assert schema_version == str(SCHEMA_VERSION) == "60"
    assert {"event_id", "event_rowid", "status", "attempts", "locked_by"} <= columns
    assert "idx_live_sim_lifecycle_inbox_status_sequence" in indexes


def test_inline_compatibility_consumes_once_and_advances_watermark(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle-inline.sqlite3")
    event = _event("evt-lifecycle-inline")
    assert append_gateway_event(connection, event).accepted is True

    first = consumer.process_live_sim_lifecycle_inline(
        connection,
        event,
        settings=Settings(),
    )
    duplicate = consumer.process_live_sim_lifecycle_inline(
        connection,
        event,
        settings=Settings(),
    )
    row = connection.execute(
        "SELECT status, attempts, consumer_source FROM live_sim_lifecycle_inbox"
    ).fetchone()
    result = connection.execute(
        """
        SELECT status, outcome, attempt_count
        FROM projection_event_results
        WHERE projection_name = 'live_sim_lifecycle' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    watermark = connection.execute(
        """
        SELECT last_success_event_id, last_error_event_id
        FROM projection_watermarks
        WHERE projection_name = 'live_sim_lifecycle'
        """
    ).fetchone()
    connection.close()

    assert first["status"] == "APPLIED"
    assert first["handler_result"]["reason"] == "missing_command_id"
    assert duplicate["status"] == "DUPLICATE"
    assert row["status"] == "APPLIED"
    assert row["attempts"] == 0
    assert row["consumer_source"] == "gateway_inline_compatibility"
    assert result["status"] == "SUCCESS"
    assert result["outcome"] == "IGNORED"
    assert result["attempt_count"] == 1
    assert watermark["last_success_event_id"] == event.event_id
    assert watermark["last_error_event_id"] is None


def test_worker_applies_lifecycle_events_in_event_rowid_order(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle-worker.sqlite3")
    first_event = _event("evt-lifecycle-worker-1")
    second_event = _event("evt-lifecycle-worker-2")
    for event in (first_event, second_event):
        append_gateway_event(connection, event)
        consumer.enqueue_live_sim_lifecycle_event(connection, event)

    result = consumer.process_live_sim_lifecycle_batch(
        connection,
        settings=_worker_settings(),
        limit=10,
    )
    rows = connection.execute(
        """
        SELECT event_id, status, consumer_source
        FROM live_sim_lifecycle_inbox
        ORDER BY event_rowid
        """
    ).fetchall()
    connection.close()

    assert result.status == "COMPLETED"
    assert result.claimed_count == 2
    assert result.applied_count == 2
    assert [row["event_id"] for row in rows] == [first_event.event_id, second_event.event_id]
    assert all(row["status"] == "APPLIED" for row in rows)
    assert all(row["consumer_source"] == "durable_worker" for row in rows)


def test_worker_rolls_back_handler_writes_then_dead_letters_and_resets(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "lifecycle-dead-letter.sqlite3")
    event = _event("evt-lifecycle-dead-letter")
    append_gateway_event(connection, event)
    consumer.enqueue_live_sim_lifecycle_event(connection, event)

    def fail_after_write(connection, event, settings=None):
        del event, settings
        connection.execute(
            """
            INSERT INTO live_sim_lifecycle_events (
                lifecycle_event_id, event_type, entity_type, evidence_json
            )
            VALUES ('must-rollback', 'TEST', 'TEST', '{}')
            """
        )
        raise RuntimeError("forced lifecycle failure")

    monkeypatch.setattr(consumer, "handle_live_sim_gateway_event", fail_after_write)
    settings = _worker_settings(live_sim_lifecycle_retry_limit=2)
    first = consumer.process_live_sim_lifecycle_batch(
        connection,
        settings=settings,
        limit=1,
    )
    second = consumer.process_live_sim_lifecycle_batch(
        connection,
        settings=settings,
        limit=1,
    )
    row = connection.execute(
        "SELECT status, attempts, last_error FROM live_sim_lifecycle_inbox"
    ).fetchone()
    rolled_back_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM live_sim_lifecycle_events
        WHERE lifecycle_event_id = 'must-rollback'
        """
    ).fetchone()["count"]
    reset = consumer.reset_live_sim_lifecycle_dead_letter(connection, event.event_id)
    reset_row = connection.execute(
        "SELECT status, attempts, last_error FROM live_sim_lifecycle_inbox"
    ).fetchone()
    connection.close()

    assert first.error_count == 1
    assert second.dead_letter_count == 1
    assert row["status"] == "DEAD_LETTER"
    assert row["attempts"] == 2
    assert "forced lifecycle failure" in row["last_error"]
    assert rolled_back_count == 0
    assert reset["status"] == "RESET"
    assert reset_row["status"] == "PENDING"
    assert reset_row["attempts"] == 0
    assert reset_row["last_error"] is None


def test_gateway_operator_and_fast_dashboard_expose_lifecycle_consumer(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "lifecycle-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    event = _event("evt-lifecycle-api")

    with TestClient(app) as client:
        accepted = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )
        duplicate = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )
        status = client.get("/api/operator/live-sim/lifecycle-consumer/status")
        inbox = client.get("/api/operator/live-sim/lifecycle-consumer/inbox?limit=10")
        operator = client.get("/api/operator/status")
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections=live_sim_lifecycle_consumer"
        )

    connection = open_connection(db_path)
    try:
        inbox_count = connection.execute(
            "SELECT COUNT(*) AS count FROM live_sim_lifecycle_inbox"
        ).fetchone()["count"]
    finally:
        connection.close()

    assert accepted.status_code == 200
    assert accepted.json()["live_sim_lifecycle"]["status"] == "APPLIED"
    assert duplicate.json()["duplicate"] is True
    assert duplicate.json()["live_sim_lifecycle"]["status"] == "DUPLICATE"
    assert status.status_code == 200
    assert status.json()["applied_count"] == 1
    assert status.json()["dead_letter_count"] == 0
    assert inbox.json()["count"] == 1
    assert operator.json()["live_sim_lifecycle_consumer"]["applied_count"] == 1
    assert dashboard.json()["live_sim_lifecycle_consumer"]["applied_count"] == 1
    assert inbox_count == 1
