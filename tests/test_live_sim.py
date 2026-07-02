from __future__ import annotations

import json

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import LiveSimIntentStatus, LiveSimOrderStatus
from gateway.command_handlers import GatewayCommandHandler
from services.config import Settings, TradingMode, TradingProfile
from services.live_sim.live_sim_service import (
    create_live_sim_intent,
    evaluate_live_sim_eligibility,
    get_live_sim_order,
    handle_live_sim_gateway_event,
    list_live_sim_cancel_intents,
    list_live_sim_exit_signals,
    list_live_sim_positions,
    queue_live_sim_order_command,
    reconcile_live_sim,
    run_live_sim_cancel_unfilled_once,
    run_live_sim_exit_once,
)
from services.live_sim.safety_gate import check_live_sim_safety_gate
from services.oms.dry_run_service import create_dry_run_intent
from storage.gateway_command_store import poll_commands
from storage.sqlite import initialize_database
from tests.test_oms_dry_run import _prepared_connection
from tests.test_oms_dry_run import _settings as _dry_run_settings


def test_live_sim_schema_and_config_defaults(tmp_path) -> None:
    connection = initialize_database(tmp_path / "live-sim-schema.sqlite3")
    table_names = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    connection.close()
    settings = Settings()

    assert {
        "live_sim_intents",
        "live_sim_orders",
        "live_sim_executions",
        "live_sim_positions",
        "live_sim_position_events",
        "live_sim_exit_signals",
        "live_sim_exit_intents",
        "live_sim_cancel_intents",
        "live_sim_lifecycle_events",
        "live_sim_rejections",
        "live_sim_runs",
        "live_sim_reconcile_snapshots",
        "live_sim_errors",
    }.issubset(table_names)
    assert settings.live_sim_enabled is False
    assert settings.live_sim_order_routing_enabled is False
    assert settings.live_sim_gateway_command_enabled is False
    assert settings.live_sim_cancel_enabled is False
    assert settings.live_sim_exit_engine_enabled is False
    assert settings.live_sim_kill_switch is True
    assert settings.live_real_allowed is False


def test_live_sim_safety_gate_defaults_and_simulation_pass(tmp_path) -> None:
    connection = initialize_database(tmp_path / "live-sim-safety.sqlite3")

    blocked = check_live_sim_safety_gate(connection, Settings())
    _mark_gateway_ready(connection)
    passed = check_live_sim_safety_gate(connection, _live_sim_settings())
    live_real = check_live_sim_safety_gate(
        connection,
        _live_sim_settings(trading_allow_live_real=True),
    )
    connection.close()

    assert blocked.passed is False
    assert LiveSimReasonCode.LIVE_SIM_DISABLED.value in blocked.reason_codes
    assert LiveSimReasonCode.LIVE_SIM_KILL_SWITCH_ACTIVE.value in blocked.reason_codes
    assert passed.passed is True
    assert passed.live_real_disabled is True
    assert live_real.passed is False
    assert LiveSimReasonCode.LIVE_REAL_NOT_ALLOWED.value in live_real.reason_codes


def test_live_sim_intent_queue_ack_execution_and_reconcile(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-flow.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()

    eligibility = evaluate_live_sim_eligibility(connection, candidate_id, settings=settings)
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    order = queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    commands = poll_commands(connection)
    events = GatewayCommandHandler().handle(commands[0])
    for event in events:
        handle_live_sim_gateway_event(connection, event, settings=settings)
    stored_order = get_live_sim_order(connection, order.live_sim_order_id)
    execution_event = _execution_event(
        command_id=commands[0].command_id,
        idempotency_key=commands[0].idempotency_key,
        broker_order_no=stored_order["broker_order_no"],
        live_sim_intent_id=intent.live_sim_intent_id,
        account_id=settings.live_sim_account_id,
    )
    execution_result = handle_live_sim_gateway_event(connection, execution_event, settings=settings)
    filled_order = get_live_sim_order(connection, order.live_sim_order_id)
    snapshot = reconcile_live_sim(connection, settings=settings)
    command_row = connection.execute(
        "SELECT command_type, source, idempotency_key, payload_json FROM gateway_commands"
    ).fetchone()
    connection.close()

    assert eligibility.eligible is True
    assert intent.status is LiveSimIntentStatus.CREATED
    assert order.status is LiveSimOrderStatus.COMMAND_QUEUED
    assert command_row["command_type"] == "send_order"
    assert command_row["source"] == "live_sim"
    assert command_row["idempotency_key"] == intent.idempotency_key
    assert stored_order["status"] == LiveSimOrderStatus.BROKER_ACKED.value
    assert stored_order["broker_order_no"].startswith("MOCKSIM-")
    assert execution_result["handled"] is True
    assert filled_order["status"] == LiveSimOrderStatus.FILLED.value
    assert snapshot.status == "LOCAL_ONLY_WITHOUT_BROKER_SNAPSHOT"


def test_live_sim_pilot_can_create_dry_run_evidence_and_intent_same_profile(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-shadow-dry-run.sqlite3")
    settings = _live_sim_settings(
        trading_profile=TradingProfile.LIVE_SIM_PILOT,
        dry_run_oms_enabled=True,
        dry_run_intent_creation_enabled=True,
        dry_run_allow_without_safety_draft_for_tests=True,
        dry_run_stale_tick_sec=999_999_999,
    )
    _mark_gateway_ready(connection)

    dry_run = create_dry_run_intent(connection, candidate_id, settings=settings)
    eligibility = evaluate_live_sim_eligibility(connection, candidate_id, settings=settings)
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert dry_run.status.value == "CREATED"
    assert eligibility.eligible is True
    assert eligibility.evidence_json["admission_trace"]["policy"] == "live_sim_intent"
    assert eligibility.evidence_json["admission_trace"]["reason_codes"] == []
    assert eligibility.evidence_json["dry_run"]["dry_run_intent_id"] == dry_run.dry_run_intent_id
    assert LiveSimReasonCode.DRY_RUN_EVIDENCE_MISSING.value not in eligibility.reason_codes
    assert intent.status is LiveSimIntentStatus.CREATED
    assert command_count == 0


def test_live_sim_partial_fill_idempotent_and_position_accounting(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-position.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()

    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    order = queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    command = poll_commands(connection)[0]
    for event in GatewayCommandHandler().handle(command):
        handle_live_sim_gateway_event(connection, event, settings=settings)
    stored_order = get_live_sim_order(connection, order.live_sim_order_id)
    partial = _execution_event(
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        broker_order_no=stored_order["broker_order_no"],
        live_sim_intent_id=intent.live_sim_intent_id,
        account_id=settings.live_sim_account_id,
        quantity=1,
        price=97_000,
        remaining_quantity=1,
        execution_id="exec-partial-1",
    )
    first = handle_live_sim_gateway_event(connection, partial, settings=settings)
    duplicate = handle_live_sim_gateway_event(connection, partial, settings=settings)
    full = _execution_event(
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        broker_order_no=stored_order["broker_order_no"],
        live_sim_intent_id=intent.live_sim_intent_id,
        account_id=settings.live_sim_account_id,
        quantity=1,
        price=99_000,
        remaining_quantity=0,
        execution_id="exec-full-1",
    )
    handle_live_sim_gateway_event(connection, full, settings=settings)
    final_order = get_live_sim_order(connection, order.live_sim_order_id)
    positions = list_live_sim_positions(connection)
    execution_count = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_executions"
    ).fetchone()["count"]
    connection.close()

    assert first["handled"] is True
    assert duplicate["duplicate"] is True
    assert execution_count == 2
    assert final_order["status"] == LiveSimOrderStatus.FILLED.value
    assert final_order["filled_quantity"] == 2
    assert final_order["avg_fill_price"] == 98_000
    assert positions[0]["quantity"] == 2
    assert positions[0]["available_quantity"] == 2
    assert positions[0]["avg_entry_price"] == 98_000


def test_live_sim_cancel_unfilled_ttl_queues_once_and_ack_cancels(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-cancel.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(
        live_sim_cancel_enabled=True,
        live_sim_cancel_unfilled_enabled=True,
        live_sim_cancel_order_ttl_sec=1,
    )
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    order = queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    old = "2020-01-01T00:00:00Z"
    connection.execute(
        """
        UPDATE live_sim_orders
        SET status = 'BROKER_ACKED',
            broker_order_no = 'MOCK-ORDER-1',
            created_at = ?,
            remaining_quantity = quantity
        WHERE live_sim_order_id = ?
        """,
        (old, order.live_sim_order_id),
    )
    connection.commit()

    first = run_live_sim_cancel_unfilled_once(
        connection,
        settings=settings,
        queue_commands=True,
    )
    second = run_live_sim_cancel_unfilled_once(
        connection,
        settings=settings,
        queue_commands=True,
    )
    cancel_command = connection.execute(
        "SELECT * FROM gateway_commands WHERE command_type = 'cancel_order'"
    ).fetchone()
    cancel_gateway_command = next(
        command
        for command in poll_commands(connection, limit=10)
        if command.command_type == "cancel_order"
    )
    events = GatewayCommandHandler().handle(cancel_gateway_command)
    for event in events:
        handle_live_sim_gateway_event(connection, event, settings=settings)
    cancelled = get_live_sim_order(connection, order.live_sim_order_id)
    cancel_intents = list_live_sim_cancel_intents(connection)
    connection.close()

    assert first.command_count == 1
    assert second.command_count == 0
    assert cancel_command["source"] == "live_sim"
    assert len(cancel_intents) == 1
    assert cancelled["status"] == LiveSimOrderStatus.CANCELLED.value


def test_live_sim_stop_loss_exit_sell_close_only_and_sell_fill_closes(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-exit.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(
        live_sim_exit_engine_enabled=True,
        live_sim_exit_order_creation_enabled=True,
        live_sim_exit_gateway_command_enabled=True,
        live_sim_exit_stop_loss_pct=3.0,
        live_sim_stale_tick_sec=999_999_999,
    )
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    order = queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    command = poll_commands(connection)[0]
    for event in GatewayCommandHandler().handle(command):
        handle_live_sim_gateway_event(connection, event, settings=settings)
    stored_order = get_live_sim_order(connection, order.live_sim_order_id)
    buy_fill = _execution_event(
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        broker_order_no=stored_order["broker_order_no"],
        live_sim_intent_id=intent.live_sim_intent_id,
        account_id=settings.live_sim_account_id,
        quantity=1,
        price=100_000,
        remaining_quantity=0,
        execution_id="exec-buy-for-exit",
    )
    handle_live_sim_gateway_event(connection, buy_fill, settings=settings)
    connection.execute("UPDATE market_ticks_latest SET price = 96000 WHERE code = '005930'")
    connection.commit()

    result = run_live_sim_exit_once(connection, settings=settings, queue_commands=True)
    exit_command = connection.execute(
        "SELECT * FROM gateway_commands WHERE command_type = 'send_order' ORDER BY created_at DESC"
    ).fetchone()
    payload = json.loads(exit_command["payload_json"])
    exit_gateway_command = next(
        command
        for command in poll_commands(connection, limit=10)
        if command.command_type == "send_order" and command.payload.get("side") == "SELL"
    )
    for event in GatewayCommandHandler().handle(exit_gateway_command):
        handle_live_sim_gateway_event(connection, event, settings=settings)
    sell_order = connection.execute(
        "SELECT * FROM live_sim_orders WHERE side = 'SELL'"
    ).fetchone()
    sell_fill = _execution_event(
        command_id=exit_gateway_command.command_id,
        idempotency_key=exit_gateway_command.idempotency_key,
        broker_order_no=sell_order["broker_order_no"],
        live_sim_intent_id=sell_order["live_sim_intent_id"],
        account_id=settings.live_sim_account_id,
        side="SELL",
        quantity=1,
        price=96_000,
        remaining_quantity=0,
        execution_id="exec-sell-close",
        metadata_extra={
            "position_id": payload["metadata"]["position_id"],
            "exit_intent_id": payload["metadata"]["exit_intent_id"],
        },
    )
    handle_live_sim_gateway_event(connection, sell_fill, settings=settings)
    positions = list_live_sim_positions(connection)
    signals = list_live_sim_exit_signals(connection)
    connection.close()

    assert result.command_count == 1
    assert payload["side"] == "SELL"
    assert payload["close_only"] is True
    assert payload["live_real_allowed"] is False
    assert signals[0]["reason"] == "STOP_LOSS"
    assert positions[0]["status"] == "CLOSED"
    assert positions[0]["quantity"] == 0
    assert positions[0]["realized_pnl"] == -4_000


def test_live_sim_default_disabled_creates_rejection_no_command(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-disabled.sqlite3")

    intent = create_live_sim_intent(connection, candidate_id, settings=Settings())
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    rejection_count = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_rejections"
    ).fetchone()["count"]
    connection.close()

    assert intent.status is LiveSimIntentStatus.REJECTED
    assert command_count == 0
    assert rejection_count == 1
    assert intent.live_real_allowed is False


def _live_sim_settings(**overrides) -> Settings:
    values = {
        "trading_mode": TradingMode.LIVE_SIM,
        "trading_allow_live_sim": True,
        "live_sim_enabled": True,
        "live_sim_order_routing_enabled": True,
        "live_sim_gateway_command_enabled": True,
        "live_sim_account_id": "SIM-12345678",
        "live_sim_kill_switch": False,
        "live_sim_stale_tick_sec": 999_999_999,
        "live_sim_max_order_notional": 100_000,
        "live_sim_max_daily_notional": 300_000,
    }
    values.update(overrides)
    return Settings(**values)


def _mark_gateway_ready(connection) -> None:
    now = datetime_to_wire(utc_now())
    for key, value in {
        "last_heartbeat_at": now,
        "gateway_orderable": "true",
        "account_mode": "SIMULATION",
        "broker_env": "SIMULATION",
        "server_mode": "SIMULATION",
        "command_queue_healthy": "true",
    }.items():
        connection.execute(
            """
            INSERT INTO gateway_status (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )
    connection.commit()


def _execution_event(
    *,
    command_id: str,
    idempotency_key: str,
    broker_order_no: str,
    live_sim_intent_id: str,
    account_id: str,
    side: str = "BUY",
    quantity: int = 1,
    price: int = 97000,
    remaining_quantity: int = 0,
    execution_id: str = "exec-live-sim-1",
    metadata_extra: dict[str, object] | None = None,
) -> GatewayEvent:
    metadata = {
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_env": "SIMULATION",
        "account_mode": "SIMULATION",
        "server_mode": "SIMULATION",
        "live_sim_intent_id": live_sim_intent_id,
        "gateway_command_id": command_id,
    }
    metadata.update(metadata_extra or {})
    return GatewayEvent(
        event_type="execution_event",
        source="mock_gateway",
        command_id=command_id,
        idempotency_key=idempotency_key,
        payload={
            "execution_id": execution_id,
            "broker_order_id": broker_order_no,
            "broker_order_no": broker_order_no,
            "account_id": account_id,
            "code": "005930",
            "side": side,
            "quantity": quantity,
            "price": price,
            "remaining_quantity": remaining_quantity,
            "executed_at": datetime_to_wire(utc_now()),
            "metadata": metadata,
        },
    )
