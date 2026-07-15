from __future__ import annotations

from datetime import UTC, datetime, timedelta

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from domain.live_sim.reasons import LiveSimReasonCode
from gateway.command_handlers import GatewayCommandHandler
from services.live_sim.safety_gate import check_live_sim_safety_gate
from storage.event_store import append_gateway_event
from storage.gateway_command_store import (
    GatewayCommandStatus,
    enqueue_command,
    expire_stale_gateway_commands,
    poll_commands,
)
from storage.gateway_order_broker_boundary import (
    get_order_broker_boundary,
    get_order_broker_boundary_status,
    list_order_broker_boundaries,
)
from storage.sqlite import (
    initialize_database,
    initialize_database_for_offline_migration,
)
from tests.test_live_sim import _live_sim_settings, _mark_gateway_ready

TS = datetime(2026, 7, 10, 9, 1, 2, tzinfo=UTC)


def _order_command(command_id: str) -> GatewayCommand:
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
            "live_sim_intent_id": f"intent-{command_id}",
            "idempotency_key": idempotency_key,
            "metadata": {
                "source": "live_sim",
                "live_sim_only": True,
                "live_real_allowed": False,
                "live_sim_intent_id": f"intent-{command_id}",
                "idempotency_key": idempotency_key,
            },
        },
        ts=TS,
    )


def _event(
    command: GatewayCommand,
    event_type: str,
    *,
    event_id: str,
    payload: dict[str, object] | None = None,
) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type=event_type,
        source="test-gateway",
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        payload=payload or {},
        ts=TS,
    )


def test_order_boundary_is_durable_and_out_of_order_safe(tmp_path) -> None:
    connection = initialize_database(tmp_path / "order-boundary.sqlite3")
    command = _order_command("cmd-boundary")
    enqueue_command(connection, command)

    assert [item.command_id for item in poll_commands(connection, limit=1)] == [
        command.command_id
    ]
    claimed = get_order_broker_boundary(connection, command.command_id)
    assert claimed is not None
    assert claimed["state"] == "CLAIMED"

    pre_ack = _event(
        command,
        "order_pre_ack",
        event_id="evt-boundary-pre-ack",
        payload={
            "status": "PRE_ACK",
            "account_id": "1234567890",
            "code": "005930",
            "side": "BUY",
        },
    )
    first = append_gateway_event(connection, pre_ack)
    duplicate = append_gateway_event(connection, pre_ack)
    append_gateway_event(
        connection,
        _event(command, "command_started", event_id="evt-boundary-started"),
    )

    after_started = get_order_broker_boundary(connection, command.command_id)
    command_status = connection.execute(
        "SELECT status FROM gateway_commands WHERE command_id = ?",
        (command.command_id,),
    ).fetchone()["status"]
    assert first.accepted is True
    assert duplicate.duplicate is True
    assert after_started is not None
    assert after_started["state"] == "PRE_ACK_RECORDED"
    assert after_started["durable_pre_ack_recorded"] is True
    assert after_started["pre_ack_payload"]["status"] == "PRE_ACK"
    assert command_status == GatewayCommandStatus.PRE_ACK_RECORDED.value

    append_gateway_event(
        connection,
        _event(
            command,
            "command_ack",
            event_id="evt-boundary-ack",
            payload={
                "details": {
                    "accepted": True,
                    "broker_order_no": "SIM-ORDER-1",
                    "broker_result_code": "0",
                }
            },
        ),
    )
    append_gateway_event(
        connection,
        _event(
            command,
            "kiwoom_order_chejan",
            event_id="evt-boundary-chejan",
            payload={"broker_order_no": "SIM-ORDER-1", "code": "005930"},
        ),
    )

    confirmed = get_order_broker_boundary(connection, command.command_id)
    status = get_order_broker_boundary_status(connection)
    assert confirmed is not None
    assert confirmed["state"] == "CHEJAN_CONFIRMED"
    assert confirmed["broker_order_no"] == "SIM-ORDER-1"
    assert confirmed["chejan_confirmed_at"] is not None
    assert status["status"] == "PASS"
    assert status["durable_pre_ack_count"] == 1
    assert status["durable_pre_ack_gap_count"] == 0
    assert len(list_order_broker_boundaries(connection)) == 1
    connection.close()


def test_order_boundary_timeout_is_unconfirmed_and_late_chejan_recovers(tmp_path) -> None:
    connection = initialize_database(tmp_path / "order-boundary-timeout.sqlite3")
    command = _order_command("cmd-timeout")
    enqueue_command(connection, command)
    poll_commands(connection, limit=1)
    append_gateway_event(
        connection,
        _event(
            command,
            "order_pre_ack",
            event_id="evt-timeout-pre-ack",
            payload={"status": "PRE_ACK"},
        ),
    )
    connection.execute(
        "UPDATE gateway_commands SET dispatched_at = ? WHERE command_id = ?",
        (
            datetime_to_wire(utc_now() - timedelta(seconds=180)),
            command.command_id,
        ),
    )
    connection.commit()

    expired = expire_stale_gateway_commands(connection, dispatched_timeout_sec=120)
    unconfirmed = get_order_broker_boundary(connection, command.command_id)
    assert expired["unconfirmed_order_count"] == 1
    assert unconfirmed is not None
    assert unconfirmed["state"] == "UNCONFIRMED"
    assert get_order_broker_boundary_status(connection)["status"] == "WARN"

    blocked_command = _order_command("cmd-blocked-by-unconfirmed")
    enqueue_command(connection, blocked_command)
    assert poll_commands(connection, limit=1, wait_sec=0) == []
    blocked_status = connection.execute(
        "SELECT status FROM gateway_commands WHERE command_id = ?",
        (blocked_command.command_id,),
    ).fetchone()["status"]
    assert blocked_status == GatewayCommandStatus.QUEUED.value

    append_gateway_event(
        connection,
        _event(
            command,
            "kiwoom_order_chejan",
            event_id="evt-timeout-late-chejan",
            payload={"broker_order_no": "SIM-LATE-1"},
        ),
    )
    recovered = get_order_broker_boundary(connection, command.command_id)
    assert recovered is not None
    assert recovered["state"] == "CHEJAN_CONFIRMED"
    assert get_order_broker_boundary_status(connection)["status"] == "PASS"
    assert [item.command_id for item in poll_commands(connection, limit=1)] == [
        blocked_command.command_id
    ]
    connection.close()


def test_pre_ack_failure_stops_command_before_broker_acceptance(tmp_path) -> None:
    connection = initialize_database(tmp_path / "order-boundary-failed.sqlite3")
    command = _order_command("cmd-pre-ack-failed")
    enqueue_command(connection, command)
    poll_commands(connection, limit=1)
    append_gateway_event(
        connection,
        _event(
            command,
            "order_pre_ack",
            event_id="evt-pre-ack-before-failure",
            payload={"status": "PRE_ACK"},
        ),
    )
    append_gateway_event(
        connection,
        _event(
            command,
            "command_failed",
            event_id="evt-pre-ack-failed",
            payload={"error_message": "DURABLE_DB_PRE_ACK_FAILED: response lost"},
        ),
    )

    command_row = connection.execute(
        "SELECT status, completed_at FROM gateway_commands WHERE command_id = ?",
        (command.command_id,),
    ).fetchone()
    boundary = get_order_broker_boundary(connection, command.command_id)
    assert command_row["status"] == GatewayCommandStatus.FAILED.value
    assert command_row["completed_at"] is not None
    assert boundary is not None
    assert boundary["state"] == "PRE_ACK_RECORDED"
    assert boundary["durable_pre_ack_recorded"] is True
    connection.close()


def test_broker_call_exception_becomes_immediately_unconfirmed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "order-boundary-uncertain.sqlite3")
    command = _order_command("cmd-broker-uncertain")
    enqueue_command(connection, command)
    poll_commands(connection, limit=1)
    append_gateway_event(
        connection,
        _event(
            command,
            "order_pre_ack",
            event_id="evt-uncertain-pre-ack",
            payload={"status": "PRE_ACK"},
        ),
    )
    append_gateway_event(
        connection,
        _event(
            command,
            "order_broker_unconfirmed",
            event_id="evt-broker-uncertain",
            payload={
                "status": "UNCONFIRMED",
                "broker_call_attempted": True,
                "broker_acceptance_unknown": True,
                "error_message": "COM transport outcome unknown",
            },
        ),
    )

    command_row = connection.execute(
        "SELECT status, completed_at FROM gateway_commands WHERE command_id = ?",
        (command.command_id,),
    ).fetchone()
    boundary = get_order_broker_boundary(connection, command.command_id)
    assert command_row["status"] == GatewayCommandStatus.UNCONFIRMED.value
    assert command_row["completed_at"] is None
    assert boundary is not None
    assert boundary["state"] == "UNCONFIRMED"
    assert boundary["durable_pre_ack_recorded"] is True
    assert get_order_broker_boundary_status(connection)["status"] == "WARN"
    connection.close()


def test_unconfirmed_boundary_blocks_new_buy_gate_but_not_lifecycle(tmp_path) -> None:
    connection = initialize_database(tmp_path / "boundary-safety-gate.sqlite3")
    _mark_gateway_ready(connection)
    command = _order_command("cmd-safety-boundary")
    enqueue_command(connection, command)
    poll_commands(connection, limit=1)
    connection.execute(
        "UPDATE gateway_commands SET status = 'UNCONFIRMED' WHERE command_id = ?",
        (command.command_id,),
    )
    connection.execute(
        """
        UPDATE gateway_order_broker_boundaries
        SET state = 'UNCONFIRMED', unconfirmed_at = updated_at
        WHERE command_id = ?
        """,
        (command.command_id,),
    )
    connection.commit()

    settings = _live_sim_settings()
    new_buy = check_live_sim_safety_gate(connection, settings, purpose="NEW_BUY")
    lifecycle = check_live_sim_safety_gate(connection, settings, purpose="LIFECYCLE")

    assert new_buy.passed is False
    assert (
        LiveSimReasonCode.ORDER_BROKER_BOUNDARY_BLOCKED.value
        in new_buy.reason_codes
    )
    assert new_buy.order_broker_boundary["status"] == "WARN"
    assert lifecycle.passed is True
    assert (
        LiveSimReasonCode.ORDER_BROKER_BOUNDARY_BLOCKED.value
        not in lifecycle.reason_codes
    )
    connection.close()


def test_mock_gateway_order_contract_records_pre_ack_before_ack(tmp_path) -> None:
    connection = initialize_database(tmp_path / "mock-boundary-contract.sqlite3")
    command = _order_command("cmd-mock-boundary")
    enqueue_command(connection, command)
    poll_commands(connection, limit=1)

    events = GatewayCommandHandler().handle(command)
    for event in events:
        append_gateway_event(connection, event)

    boundary = get_order_broker_boundary(connection, command.command_id)
    assert [event.event_type for event in events] == [
        "command_started",
        "order_pre_ack",
        "command_ack",
    ]
    assert boundary is not None
    assert boundary["state"] == "BROKER_ACCEPTED"
    assert boundary["durable_pre_ack_recorded"] is True
    assert get_order_broker_boundary_status(connection)["status"] == "PASS"
    connection.close()


def test_legacy_order_boundary_migration_and_rerun_are_idempotent(tmp_path) -> None:
    db_path = tmp_path / "legacy-order-boundary.sqlite3"
    connection = initialize_database(db_path)
    command = _order_command("cmd-legacy")
    enqueue_command(connection, command)
    poll_commands(connection, limit=1)
    append_gateway_event(
        connection,
        _event(
            command,
            "order_pre_ack",
            event_id="evt-legacy-pre-ack",
            payload={"status": "PRE_ACK"},
        ),
    )
    append_gateway_event(
        connection,
        _event(
            command,
            "command_ack",
            event_id="evt-legacy-ack",
            payload={"details": {"broker_order_no": "SIM-LEGACY-1"}},
        ),
    )
    connection.execute("DROP TABLE gateway_order_broker_boundaries")
    connection.execute(
        "UPDATE app_metadata SET value = '46' WHERE key = 'schema_version'"
    )
    connection.commit()
    connection.close()

    migrated = initialize_database_for_offline_migration(db_path)
    boundary = get_order_broker_boundary(migrated, command.command_id)
    assert boundary is not None
    assert boundary["state"] == "BROKER_ACCEPTED"
    assert boundary["durable_pre_ack_recorded"] is True
    assert boundary["broker_order_no"] == "SIM-LEGACY-1"
    assert get_order_broker_boundary_status(migrated)["status"] == "PASS"
    migrated.close()

    rerun = initialize_database(db_path)
    assert len(list_order_broker_boundaries(rerun)) == 1
    assert get_order_broker_boundary_status(rerun)["status"] == "PASS"
    rerun.close()


def test_legacy_ack_without_pre_ack_fails_closed(tmp_path) -> None:
    db_path = tmp_path / "legacy-order-boundary-gap.sqlite3"
    connection = initialize_database(db_path)
    command = _order_command("cmd-legacy-gap")
    enqueue_command(connection, command)
    poll_commands(connection, limit=1)
    append_gateway_event(
        connection,
        _event(
            command,
            "command_ack",
            event_id="evt-legacy-gap-ack",
            payload={"details": {"broker_order_no": "SIM-GAP-1"}},
        ),
    )
    connection.execute("DROP TABLE gateway_order_broker_boundaries")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    boundary = get_order_broker_boundary(migrated, command.command_id)
    status = get_order_broker_boundary_status(migrated)
    assert boundary is not None
    assert boundary["state"] == "BROKER_ACCEPTED"
    assert boundary["durable_pre_ack_recorded"] is False
    assert status["status"] == "FAIL"
    assert status["durable_pre_ack_gap_count"] == 1
    assert "DURABLE_PRE_ACK_GAP" in status["reason_codes"]
    assert status["block_new_order_routing"] is True
    migrated.close()


def test_gateway_event_boundary_index_excludes_market_data_only_rows(tmp_path) -> None:
    connection = initialize_database(tmp_path / "boundary-index.sqlite3")
    index_sql = connection.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'index'
          AND name = 'idx_gateway_events_command_event'
        """
    ).fetchone()["sql"]

    assert "WHERE command_id IS NOT NULL" in index_sql
    connection.close()
