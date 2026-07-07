from __future__ import annotations

from datetime import UTC, datetime, timedelta

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from storage.event_store import append_gateway_event
from storage.gateway_command_store import (
    GatewayCommandStatus,
    enqueue_command,
    expire_stale_gateway_commands,
    get_command_status_counts,
    poll_commands,
)
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def make_command(
    command_id: str,
    *,
    command_type: str = "request_tr",
    idempotency_key: str | None = None,
) -> GatewayCommand:
    return GatewayCommand(
        command_id=command_id,
        command_type=command_type,
        source="core",
        payload={"request_id": command_id, "tr_code": "OPT10001", "params": {"code": "005930"}},
        idempotency_key=idempotency_key,
        ts=TS,
    )


def make_live_sim_order_command(command_id: str) -> GatewayCommand:
    idempotency_key = f"idem-{command_id}"
    return GatewayCommand(
        command_id=command_id,
        command_type="send_order",
        source="live_sim",
        idempotency_key=idempotency_key,
        payload={
            "account_id": "1234567890",
            "account_mode": "SIMULATION",
            "broker_env": "SIMULATION",
            "server_mode": "SIMULATION",
            "code": "005930",
            "side": "BUY",
            "quantity": 1,
            "price": 70000,
            "mode": "LIVE_SIM",
            "live_mode": "LIVE_SIM",
            "live_sim_intent_id": "intent-1",
            "idempotency_key": idempotency_key,
            "metadata": {
                "source": "live_sim",
                "live_sim_only": True,
                "live_real_allowed": False,
                "live_sim_intent_id": "intent-1",
                "idempotency_key": idempotency_key,
            },
        },
        ts=TS,
    )


def test_enqueue_command_then_poll_dispatches_and_increments_attempts(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")

    result = enqueue_command(connection, make_command("cmd_poll"))
    commands = poll_commands(connection, limit=20, wait_sec=0)

    row = connection.execute(
        """
        SELECT status, attempts, dispatched_at
        FROM gateway_commands
        WHERE command_id = 'cmd_poll'
        """
    ).fetchone()
    connection.close()

    assert result.accepted is True
    assert [command.command_id for command in commands] == ["cmd_poll"]
    assert row["status"] == GatewayCommandStatus.DISPATCHED.value
    assert row["attempts"] == 1
    assert row["dispatched_at"] is not None


def test_polled_command_carries_expires_at_metadata(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    expires_at = utc_now() + timedelta(seconds=30)

    enqueue_command(connection, make_command("cmd_with_expiry"), expires_at=expires_at)
    command = poll_commands(connection, limit=1, wait_sec=0)[0]
    connection.close()

    assert command.payload["_gateway_command_expires_at"] == datetime_to_wire(expires_at)


def test_idempotency_key_prevents_duplicate_enqueue(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")

    first = enqueue_command(connection, make_command("cmd_first", idempotency_key="same-key"))
    second = enqueue_command(connection, make_command("cmd_second", idempotency_key="same-key"))

    row_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert first.accepted is True
    assert second.accepted is False
    assert second.duplicate is True
    assert row_count == 1


def test_command_ack_event_marks_command_acked(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(connection, make_command("cmd_ack"))
    poll_commands(connection)

    append_gateway_event(
        connection,
        GatewayEvent(
            event_id="evt_ack",
            event_type="command_ack",
            source="test-gateway",
            command_id="cmd_ack",
            payload={"ok": True},
            ts=TS,
        ),
    )

    command_row = connection.execute(
        "SELECT status, completed_at FROM gateway_commands WHERE command_id = 'cmd_ack'"
    ).fetchone()
    event_row = connection.execute(
        "SELECT status FROM gateway_command_events WHERE command_id = 'cmd_ack'"
    ).fetchone()
    connection.close()

    assert command_row["status"] == GatewayCommandStatus.ACKED.value
    assert command_row["completed_at"] is not None
    assert event_row["status"] == GatewayCommandStatus.ACKED.value


def test_command_failed_event_marks_command_failed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(connection, make_command("cmd_failed"))

    append_gateway_event(
        connection,
        GatewayEvent(
            event_id="evt_failed",
            event_type="command_failed",
            source="test-gateway",
            command_id="cmd_failed",
            payload={"error_message": "TR rejected"},
            ts=TS,
        ),
    )

    row = connection.execute(
        """
        SELECT status, completed_at, last_error
        FROM gateway_commands
        WHERE command_id = 'cmd_failed'
        """
    ).fetchone()
    connection.close()

    assert row["status"] == GatewayCommandStatus.FAILED.value
    assert row["completed_at"] is not None
    assert row["last_error"] == "TR rejected"


def test_rate_limited_event_requeues_command_with_available_at(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(connection, make_command("cmd_rate_limited"))
    poll_commands(connection)

    append_gateway_event(
        connection,
        GatewayEvent(
            event_id="evt_rate_limited",
            event_type="rate_limited",
            source="test-gateway",
            command_id="cmd_rate_limited",
            payload={"wait_time_sec": 60, "reason": "global_per_second"},
            ts=TS,
        ),
    )

    commands = poll_commands(connection, wait_sec=0)
    row = connection.execute(
        """
        SELECT status, available_at, attempts, completed_at, last_error
        FROM gateway_commands
        WHERE command_id = 'cmd_rate_limited'
        """
    ).fetchone()
    connection.close()

    assert commands == []
    assert row["status"] == GatewayCommandStatus.QUEUED.value
    assert row["available_at"] is not None
    assert row["attempts"] == 1
    assert row["completed_at"] is None
    assert row["last_error"] is None


def test_expired_command_is_not_polled(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(
        connection,
        make_command("cmd_expired"),
        expires_at=utc_now() - timedelta(seconds=1),
    )

    commands = poll_commands(connection)

    row = connection.execute(
        "SELECT status, attempts FROM gateway_commands WHERE command_id = 'cmd_expired'"
    ).fetchone()
    connection.close()

    assert commands == []
    assert row["status"] == GatewayCommandStatus.EXPIRED.value
    assert row["attempts"] == 0


def test_order_commands_are_claimed_before_market_data_backlog(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(connection, make_command("cmd_request_old", command_type="request_tr"))
    enqueue_command(
        connection,
        make_command("cmd_realtime_old", command_type="register_realtime"),
    )
    enqueue_command(connection, make_live_sim_order_command("cmd_send_new"))

    commands = poll_commands(connection, limit=1, wait_sec=0)
    rows = connection.execute(
        """
        SELECT command_id, status, attempts
        FROM gateway_commands
        ORDER BY command_id ASC
        """
    ).fetchall()
    connection.close()

    assert [command.command_id for command in commands] == ["cmd_send_new"]
    statuses = {row["command_id"]: row["status"] for row in rows}
    attempts = {row["command_id"]: row["attempts"] for row in rows}
    assert statuses["cmd_send_new"] == GatewayCommandStatus.DISPATCHED.value
    assert attempts["cmd_send_new"] == 1
    assert statuses["cmd_request_old"] == GatewayCommandStatus.QUEUED.value
    assert statuses["cmd_realtime_old"] == GatewayCommandStatus.QUEUED.value


def test_stale_dispatched_command_is_failed_and_queue_health_refreshed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(connection, make_command("cmd_stale_dispatched"))
    poll_commands(connection)
    connection.execute(
        """
        UPDATE gateway_commands
        SET dispatched_at = ?
        WHERE command_id = 'cmd_stale_dispatched'
        """,
        (datetime_to_wire(utc_now() - timedelta(seconds=180)),),
    )
    connection.execute(
        """
        INSERT INTO gateway_status (key, value, updated_at)
        VALUES ('command_queue_healthy', 'false', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (datetime_to_wire(utc_now()),),
    )
    connection.commit()

    result = expire_stale_gateway_commands(connection, dispatched_timeout_sec=120)
    counts = get_command_status_counts(connection)
    row = connection.execute(
        """
        SELECT status, completed_at, last_error
        FROM gateway_commands
        WHERE command_id = 'cmd_stale_dispatched'
        """
    ).fetchone()
    status = connection.execute(
        "SELECT value FROM gateway_status WHERE key = 'command_queue_healthy'"
    ).fetchone()
    connection.close()

    assert result["timed_out_dispatched_count"] == 1
    assert counts[GatewayCommandStatus.FAILED.value] == 1
    assert row["status"] == GatewayCommandStatus.FAILED.value
    assert row["completed_at"] is not None
    assert "timed out" in row["last_error"]
    assert status["value"] == "true"


def test_command_status_counts_are_read_only_for_queue_health(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    connection.execute(
        """
        INSERT INTO gateway_status (key, value, updated_at)
        VALUES ('command_queue_healthy', 'false', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (datetime_to_wire(utc_now()),),
    )
    connection.commit()

    counts = get_command_status_counts(connection)
    status = connection.execute(
        "SELECT value FROM gateway_status WHERE key = 'command_queue_healthy'"
    ).fetchone()
    connection.close()

    assert counts[GatewayCommandStatus.QUEUED.value] == 0
    assert status["value"] == "false"


def test_idle_poll_commands_does_not_require_write_lock(tmp_path) -> None:
    db_path = tmp_path / "commands.sqlite3"
    initialize_database(db_path).close()
    writer = initialize_database(db_path)
    reader = initialize_database(db_path)
    writer.execute("BEGIN IMMEDIATE")

    commands = poll_commands(reader, wait_sec=0)

    writer.rollback()
    writer.close()
    reader.close()

    assert commands == []


def test_stale_dispatched_order_becomes_unconfirmed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(connection, make_live_sim_order_command("cmd_stale_order"))
    poll_commands(connection)
    connection.execute(
        """
        UPDATE gateway_commands
        SET dispatched_at = ?
        WHERE command_id = 'cmd_stale_order'
        """,
        (datetime_to_wire(utc_now() - timedelta(seconds=180)),),
    )
    connection.commit()

    result = expire_stale_gateway_commands(connection, dispatched_timeout_sec=120)
    counts = get_command_status_counts(connection)
    row = connection.execute(
        """
        SELECT status, completed_at, last_error
        FROM gateway_commands
        WHERE command_id = 'cmd_stale_order'
        """
    ).fetchone()
    connection.close()

    assert result["timed_out_dispatched_count"] == 0
    assert result["unconfirmed_order_count"] == 1
    assert counts[GatewayCommandStatus.UNCONFIRMED.value] == 1
    assert row["status"] == GatewayCommandStatus.UNCONFIRMED.value
    assert row["completed_at"] is None
    assert "reconciliation required" in row["last_error"]


def test_execution_event_reconciles_unconfirmed_order(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")
    enqueue_command(connection, make_live_sim_order_command("cmd_reconciled_order"))
    poll_commands(connection)
    connection.execute(
        """
        UPDATE gateway_commands
        SET status = ?, completed_at = NULL
        WHERE command_id = 'cmd_reconciled_order'
        """,
        (GatewayCommandStatus.UNCONFIRMED.value,),
    )
    connection.commit()

    append_gateway_event(
        connection,
        GatewayEvent(
            event_id="evt_execution_reconcile",
            event_type="execution_event",
            source="test-gateway",
            command_id="cmd_reconciled_order",
            payload={
                "execution_id": "exec-1",
                "broker_order_id": "broker-1",
                "code": "005930",
                "side": "BUY",
                "quantity": 1,
                "price": 70000,
                "executed_at": datetime_to_wire(TS),
            },
            ts=TS,
        ),
    )

    row = connection.execute(
        """
        SELECT status, completed_at
        FROM gateway_commands
        WHERE command_id = 'cmd_reconciled_order'
        """
    ).fetchone()
    connection.close()

    assert row["status"] == GatewayCommandStatus.ACKED.value
    assert row["completed_at"] is not None


def test_forbidden_order_command_type_is_rejected(tmp_path) -> None:
    connection = initialize_database(tmp_path / "commands.sqlite3")

    result = enqueue_command(
        connection,
        make_command("cmd_order", command_type="send_order"),
    )

    row_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert result.accepted is False
    assert result.status == GatewayCommandStatus.REJECTED
    assert "disabled" in result.error_message
    assert row_count == 0
