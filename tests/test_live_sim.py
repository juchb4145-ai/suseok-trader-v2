from __future__ import annotations

import json
from datetime import UTC, datetime

import domain.broker.utils as broker_utils
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
    run_live_sim_reprice_once,
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


def test_live_sim_daily_loss_blocks_buy_intent_and_command(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-daily-loss.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(live_sim_max_daily_loss=100_000)
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="CLOSED",
        quantity=0,
        realized_pnl=-120_000,
    )

    eligibility = evaluate_live_sim_eligibility(connection, candidate_id, settings=settings)
    rejected_intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    connection.execute("DELETE FROM live_sim_positions")
    connection.commit()
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="CLOSED",
        quantity=0,
        realized_pnl=-120_000,
    )
    try:
        queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    except ValueError as exc:
        queue_error = str(exc)
    else:
        raise AssertionError("expected daily loss to block LIVE_SIM BUY command queue")
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert LiveSimReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value in eligibility.reason_codes
    assert rejected_intent.status is LiveSimIntentStatus.REJECTED
    assert LiveSimReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value in rejected_intent.reason_codes
    assert LiveSimReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value in queue_error
    assert command_count == 0


def test_live_sim_entry_window_blocks_buy_and_records_rejection(
    tmp_path,
    monkeypatch,
) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-entry-window.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(
        live_sim_entry_window_start="09:05:00",
        live_sim_entry_window_end="14:30:00",
        live_sim_exit_eod_flatten_time="15:15:00",
    )
    monkeypatch.setattr(
        broker_utils,
        "utc_now",
        lambda: datetime(2026, 7, 1, 0, 4, 59, tzinfo=UTC),
    )

    eligibility = evaluate_live_sim_eligibility(connection, candidate_id, settings=settings)
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    rejection = connection.execute(
        """
        SELECT reason_codes_json, evidence_json
        FROM live_sim_rejections
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    connection.close()

    assert eligibility.eligible is False
    assert LiveSimReasonCode.ENTRY_WINDOW_CLOSED.value in eligibility.reason_codes
    assert intent.status is LiveSimIntentStatus.REJECTED
    assert LiveSimReasonCode.ENTRY_WINDOW_CLOSED.value in intent.reason_codes
    assert LiveSimReasonCode.ENTRY_WINDOW_CLOSED.value in json.loads(
        rejection["reason_codes_json"]
    )
    evidence = json.loads(rejection["evidence_json"])
    assert evidence["entry_window"]["current_time"] == "09:04:59"
    assert evidence["entry_window"]["open"] is False


def test_live_sim_entry_window_allows_boundaries_and_blocks_after_end(
    tmp_path,
    monkeypatch,
) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-entry-boundary.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(
        live_sim_entry_window_start="09:05:00",
        live_sim_entry_window_end="14:30:00",
        live_sim_exit_eod_flatten_time="15:15:00",
    )

    monkeypatch.setattr(
        broker_utils,
        "utc_now",
        lambda: datetime(2026, 7, 1, 0, 5, 0, tzinfo=UTC),
    )
    at_start = evaluate_live_sim_eligibility(connection, candidate_id, settings=settings)
    monkeypatch.setattr(
        broker_utils,
        "utc_now",
        lambda: datetime(2026, 7, 1, 5, 30, 0, tzinfo=UTC),
    )
    at_end = evaluate_live_sim_eligibility(connection, candidate_id, settings=settings)
    monkeypatch.setattr(
        broker_utils,
        "utc_now",
        lambda: datetime(2026, 7, 1, 5, 30, 1, tzinfo=UTC),
    )
    after_end = evaluate_live_sim_eligibility(connection, candidate_id, settings=settings)
    connection.close()

    assert at_start.eligible is True
    assert at_end.eligible is True
    assert after_end.eligible is False
    assert LiveSimReasonCode.ENTRY_WINDOW_CLOSED.value in after_end.reason_codes


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


def test_live_sim_buy_limit_price_uses_krx_tick_offset(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-buy-offset.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(live_sim_buy_price_offset_ticks=1)

    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    order = queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    command_row = connection.execute(
        "SELECT payload_json FROM gateway_commands WHERE command_type = 'send_order'"
    ).fetchone()
    connection.close()

    payload = json.loads(command_row["payload_json"])
    assert intent.limit_price == 97_100
    assert intent.notional == 97_100
    assert order.limit_price == 97_100
    assert payload["price"] == 97_100
    assert intent.evidence_json["price_policy"]["buy_price_offset_ticks"] == 1


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


def test_live_sim_position_pnl_is_net_of_buy_fee_sell_fee_and_tax(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-net-pnl.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(live_sim_fee_rate=0.0035, live_sim_tax_rate=0.0018)
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
        execution_id="exec-net-buy",
    )
    handle_live_sim_gateway_event(connection, buy_fill, settings=settings)
    position = list_live_sim_positions(connection)[0]
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id,
            live_sim_intent_id,
            gateway_command_id,
            trade_date,
            account_id,
            code,
            name,
            side,
            order_type,
            quantity,
            limit_price,
            notional,
            status,
            broker_order_no,
            filled_quantity,
            remaining_quantity,
            idempotency_key
        )
        VALUES ('sell-net-order', 'sell-net-intent', 'sell-net-command',
            '2026-06-27', 'SIM-12345678', '005930', '삼성전자', 'SELL', 'LIMIT',
            1, 110000, 110000, 'BROKER_ACKED', 'SELL-NET-1', 0, 1, 'sell-net-key')
        """
    )
    connection.commit()
    sell_fill = _execution_event(
        command_id="sell-net-command",
        idempotency_key="sell-net-key",
        broker_order_no="SELL-NET-1",
        live_sim_intent_id="sell-net-intent",
        account_id=settings.live_sim_account_id,
        side="SELL",
        quantity=1,
        price=110_000,
        remaining_quantity=0,
        execution_id="exec-net-sell",
        metadata_extra={"position_id": position["position_id"]},
    )
    handle_live_sim_gateway_event(connection, sell_fill, settings=settings)
    closed = list_live_sim_positions(connection, open_only=False)[0]
    connection.close()

    assert round(position["avg_entry_price"], 6) == 100_350
    assert closed["status"] == "CLOSED"
    assert round(closed["realized_pnl"], 6) == 9_067


def test_live_sim_reconcile_notional_tolerance_ignores_rounding_noise(tmp_path) -> None:
    connection, _ = _prepared_connection(tmp_path / "live-sim-reconcile-tolerance.sqlite3")
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="OPEN",
        quantity=1,
        realized_pnl=0,
    )
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_executions (
            live_sim_execution_id,
            account_id,
            code,
            side,
            quantity,
            price,
            notional,
            executed_at,
            raw_event_json
        )
        VALUES ('exec-reconcile-tolerance', 'SIM-12345678', '005930', 'BUY',
            1, 97000, 97000, ?, '{}')
        """,
        (now,),
    )
    connection.execute(
        "UPDATE live_sim_positions SET total_entry_notional = 97000.5"
    )
    connection.commit()

    tolerated = reconcile_live_sim(
        connection,
        settings=_live_sim_settings(live_sim_reconcile_notional_tolerance=1.0),
    )
    connection.execute(
        "UPDATE live_sim_positions SET total_entry_notional = 97002.1"
    )
    connection.commit()
    mismatch = reconcile_live_sim(
        connection,
        settings=_live_sim_settings(live_sim_reconcile_notional_tolerance=1.0),
    )
    connection.close()

    assert tolerated.mismatch_count == 0
    assert mismatch.mismatch_count == 1
    assert mismatch.snapshot_json["mismatches"][0]["reason"] == (
        "position_entry_notional_mismatch"
    )


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


def test_live_sim_reprice_retries_ttl_cancelled_buy_once(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-reprice.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(
        live_sim_cancel_enabled=True,
        live_sim_cancel_unfilled_enabled=True,
        live_sim_cancel_order_ttl_sec=1,
        live_sim_reprice_enabled=True,
        live_sim_reprice_max_attempts=1,
    )
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    order = queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    buy_command = poll_commands(connection)[0]
    for event in GatewayCommandHandler().handle(buy_command):
        handle_live_sim_gateway_event(connection, event, settings=settings)
    old = "2020-01-01T00:00:00Z"
    connection.execute(
        """
        UPDATE live_sim_orders
        SET created_at = ?,
            remaining_quantity = quantity
        WHERE live_sim_order_id = ?
        """,
        (old, order.live_sim_order_id),
    )
    connection.commit()
    run_live_sim_cancel_unfilled_once(connection, settings=settings, queue_commands=True)
    cancel_gateway_command = next(
        command
        for command in poll_commands(connection, limit=10)
        if command.command_type == "cancel_order"
    )
    for event in GatewayCommandHandler().handle(cancel_gateway_command):
        handle_live_sim_gateway_event(connection, event, settings=settings)
    connection.execute("UPDATE market_ticks_latest SET price = 98000 WHERE code = '005930'")
    connection.commit()

    first = run_live_sim_reprice_once(connection, settings=settings, queue_commands=True)
    second = run_live_sim_reprice_once(connection, settings=settings, queue_commands=True)
    commands = connection.execute(
        """
        SELECT payload_json
        FROM gateway_commands
        WHERE command_type = 'send_order'
        ORDER BY created_at ASC
        """
    ).fetchall()
    intents = connection.execute(
        "SELECT status, evidence_json FROM live_sim_intents ORDER BY created_at ASC"
    ).fetchall()
    connection.close()

    retry_payload = json.loads(commands[-1]["payload_json"])
    retry_evidence = json.loads(intents[-1]["evidence_json"])
    assert first.command_count == 1
    assert second.command_count == 0
    assert retry_payload["side"] == "BUY"
    assert retry_payload["price"] == 98_100
    assert retry_evidence["source"] == "live_sim_reprice"
    assert retry_evidence["reprice"]["attempt"] == 1
    assert intents[0]["status"] == LiveSimIntentStatus.CANCELLED.value


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
        live_sim_max_daily_loss=1,
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
    connection.execute("UPDATE live_sim_positions SET unrealized_pnl = -4000")
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


def test_live_sim_entry_window_does_not_block_exit_sell_close_only(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _ = _prepared_connection(tmp_path / "live-sim-exit-window.sqlite3")
    _mark_gateway_ready(connection)
    settings = _live_sim_settings(
        live_sim_entry_window_start="09:05:00",
        live_sim_entry_window_end="14:30:00",
        live_sim_exit_eod_flatten_time="15:15:00",
        live_sim_exit_engine_enabled=True,
        live_sim_exit_order_creation_enabled=True,
        live_sim_exit_gateway_command_enabled=True,
        live_sim_exit_stop_loss_pct=3.0,
    )
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="OPEN",
        quantity=1,
        realized_pnl=0,
    )
    connection.execute("UPDATE market_ticks_latest SET price = 90000 WHERE code = '005930'")
    connection.commit()
    monkeypatch.setattr(
        broker_utils,
        "utc_now",
        lambda: datetime(2026, 7, 1, 5, 31, 0, tzinfo=UTC),
    )

    result = run_live_sim_exit_once(connection, settings=settings, queue_commands=True)
    exit_command = connection.execute(
        "SELECT payload_json FROM gateway_commands WHERE command_type = 'send_order'"
    ).fetchone()
    exit_intent = connection.execute("SELECT status FROM live_sim_exit_intents").fetchone()
    connection.close()

    payload = json.loads(exit_command["payload_json"])
    assert result.command_count == 1
    assert payload["side"] == "SELL"
    assert payload["close_only"] is True
    assert exit_intent["status"] == "COMMAND_QUEUED"


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


def _insert_live_sim_position(
    connection,
    *,
    trade_date: str,
    status: str,
    quantity: int,
    realized_pnl: float,
    unrealized_pnl: float = 0.0,
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id,
            account_id,
            trade_date,
            code,
            name,
            side,
            quantity,
            available_quantity,
            avg_entry_price,
            total_entry_notional,
            realized_pnl,
            unrealized_pnl,
            opened_at,
            closed_at,
            last_price,
            last_price_at,
            status,
            created_at,
            updated_at
        )
        VALUES ('live-sim-loss-position', 'SIM-12345678', ?, '005930', '삼성전자',
            'LONG', ?, ?, 97000, ?, ?, ?, ?, ?, 97000, ?, ?, ?, ?)
        """,
        (
            trade_date,
            quantity,
            quantity,
            97_000 * quantity,
            realized_pnl,
            unrealized_pnl,
            now,
            now if status == "CLOSED" else None,
            now,
            status,
            now,
            now,
        ),
    )
    connection.commit()


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
        "live_sim_entry_window_start": "00:00:00",
        "live_sim_entry_window_end": "23:59:58",
        "live_sim_exit_eod_flatten_time": "23:59:59",
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
