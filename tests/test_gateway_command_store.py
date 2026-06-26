from __future__ import annotations

from datetime import UTC, datetime, timedelta

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import utc_now
from storage.event_store import append_gateway_event
from storage.gateway_command_store import (
    GatewayCommandStatus,
    enqueue_command,
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
