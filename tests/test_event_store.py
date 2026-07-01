from __future__ import annotations

from datetime import UTC, datetime, timedelta

from domain.broker.events import GatewayEvent
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json, hash_payload_json
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def test_gateway_event_is_stored_in_raw_and_gateway_tables(tmp_path) -> None:
    connection = initialize_database(tmp_path / "events.sqlite3")
    event = GatewayEvent(
        event_id="evt_heartbeat_1",
        event_type="heartbeat",
        source="test-gateway",
        payload={"status": "ok"},
        command_id="cmd_heartbeat_1",
        idempotency_key="heartbeat-once",
        ts=TS,
    )

    result = append_gateway_event(connection, event)

    raw_row = connection.execute(
        "SELECT * FROM raw_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    gateway_row = connection.execute(
        "SELECT * FROM gateway_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    status_rows = connection.execute("SELECT key, value FROM gateway_status").fetchall()
    connection.close()

    assert result.accepted is True
    assert result.duplicate is False
    assert raw_row["event_type"] == "heartbeat"
    assert raw_row["command_id"] == "cmd_heartbeat_1"
    assert raw_row["idempotency_key"] == "heartbeat-once"
    assert raw_row["payload_hash"] == hash_payload_json(raw_row["payload_json"])
    assert gateway_row["status"] == "ACCEPTED"
    status_values = {row["key"]: row["value"] for row in status_rows}
    assert status_values["last_heartbeat_at"] == event.to_dict()["ts"]


def test_duplicate_event_with_same_payload_increments_duplicate_count(tmp_path) -> None:
    connection = initialize_database(tmp_path / "events.sqlite3")
    event = GatewayEvent(
        event_id="evt_duplicate",
        event_type="heartbeat",
        source="test-gateway",
        payload={"status": "ok"},
        ts=TS,
    )

    first = append_gateway_event(connection, event)
    second = append_gateway_event(connection, event)

    row = connection.execute(
        "SELECT duplicate_count FROM raw_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    connection.close()

    assert first.duplicate is False
    assert second.accepted is True
    assert second.duplicate is True
    assert row["duplicate_count"] == 1


def test_older_heartbeat_does_not_overwrite_latest_gateway_status(tmp_path) -> None:
    connection = initialize_database(tmp_path / "events.sqlite3")
    latest = GatewayEvent(
        event_id="evt_heartbeat_latest",
        event_type="heartbeat",
        source="test-gateway",
        payload={"registered_realtime_code_count": 87, "condition_load_state": "LOADED"},
        ts=TS + timedelta(seconds=10),
    )
    older = GatewayEvent(
        event_id="evt_heartbeat_older",
        event_type="heartbeat",
        source="test-gateway",
        payload={"registered_realtime_code_count": 0, "condition_load_state": "IDLE"},
        ts=TS,
    )

    append_gateway_event(connection, latest)
    append_gateway_event(connection, older)

    status_rows = connection.execute("SELECT key, value FROM gateway_status").fetchall()
    connection.close()

    status_values = {row["key"]: row["value"] for row in status_rows}
    assert status_values["last_heartbeat_at"] == latest.to_dict()["ts"]
    assert status_values["registered_realtime_code_count"] == "87"
    assert status_values["condition_load_state"] == "LOADED"


def test_duplicate_event_with_different_payload_is_conflict(tmp_path) -> None:
    connection = initialize_database(tmp_path / "events.sqlite3")
    event = GatewayEvent(
        event_id="evt_conflict",
        event_type="heartbeat",
        source="test-gateway",
        payload={"status": "ok"},
        ts=TS,
    )
    conflicting_event = GatewayEvent(
        event_id="evt_conflict",
        event_type="heartbeat",
        source="test-gateway",
        payload={"status": "changed"},
        ts=TS,
    )

    append_gateway_event(connection, event)
    result = append_gateway_event(connection, conflicting_event)

    row_count = connection.execute("SELECT COUNT(*) AS count FROM raw_events").fetchone()["count"]
    connection.close()

    assert result.accepted is False
    assert result.status == "CONFLICT"
    assert row_count == 1


def test_payload_hash_is_deterministic_for_canonical_json() -> None:
    payload_a = {"b": 2, "a": {"c": 1}}
    payload_b = {"a": {"c": 1}, "b": 2}

    assert canonical_json(payload_a) == canonical_json(payload_b)
    assert hash_payload_json(canonical_json(payload_a)) == hash_payload_json(
        canonical_json(payload_b)
    )


def test_unknown_event_type_is_stored_with_warning_status(tmp_path) -> None:
    connection = initialize_database(tmp_path / "events.sqlite3")
    event = GatewayEvent(
        event_id="evt_unknown",
        event_type="future_event",
        source="test-gateway",
        payload={"value": 1},
        ts=TS,
    )

    result = append_gateway_event(connection, event)

    row = connection.execute(
        "SELECT status, error_message FROM gateway_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    connection.close()

    assert result.accepted is True
    assert result.status == "UNKNOWN_EVENT_TYPE"
    assert row["status"] == "UNKNOWN_EVENT_TYPE"
    assert "future_event" in row["error_message"]


def test_quote_tick_is_supported_without_price_projection_validation(tmp_path) -> None:
    connection = initialize_database(tmp_path / "events.sqlite3")
    event = GatewayEvent(
        event_id="evt_quote_tick",
        event_type="quote_tick",
        source="test-gateway",
        payload={"code": "005930", "best_bid": 70000, "best_ask": 70100},
        ts=TS,
    )

    result = append_gateway_event(connection, event)

    row = connection.execute(
        "SELECT status, error_message FROM gateway_events WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    connection.close()

    assert result.accepted is True
    assert result.status == "ACCEPTED"
    assert row["status"] == "ACCEPTED"
    assert row["error_message"] is None
