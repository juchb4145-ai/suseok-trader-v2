from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from domain.broker.account_snapshot import mask_account_id
from domain.broker.commands import GatewayCommand
from domain.broker.utils import datetime_to_wire, market_today, utc_now
from gateway.kiwoom_command_handlers import KiwoomGatewayCommandHandler
from services.live_sim.live_sim_service import (
    _latest_reconcile_blocks_new_buy,
    queue_live_sim_order_command,
    reconcile_live_sim,
    request_live_sim_broker_snapshot,
)
from storage.gateway_command_store import poll_commands
from storage.sqlite import initialize_database
from tests.test_live_sim import _live_sim_settings


class _SnapshotClient:
    def __init__(self, server_gubun: str = "1") -> None:
        self.server_gubun = server_gubun

    def get_server_gubun(self) -> str:
        return self.server_gubun


class _SnapshotTrRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def submit(self, **kwargs):
        self.calls.append(dict(kwargs))
        section = str(kwargs["request_name"])
        continuation = kwargs.get("continuation_key")
        if section.endswith("open_orders") and continuation is None:
            rows = [
                {
                    "계좌번호": "99991234",
                    "주문번호": "BROKER-1",
                    "종목코드": "005930",
                    "종목명": "삼성전자",
                    "주문상태": "접수",
                    "주문수량": "2",
                    "미체결수량": "1",
                    "체결량": "1",
                    "주문가격": "70000",
                    "주문구분": "+매수",
                }
            ]
            next_key = "2"
        elif section.endswith("open_orders"):
            rows = []
            next_key = ""
        elif section.endswith("executions"):
            rows = [
                {
                    "주문번호": "BROKER-1",
                    "체결번호": "EXEC-1",
                    "종목코드": "005930",
                    "체결량": "1",
                    "체결가": "70000",
                    "주문구분": "+매수",
                }
            ]
            next_key = ""
        else:
            rows = [
                {
                    "종목번호": "A005930",
                    "종목명": "삼성전자",
                    "보유수량": "1",
                    "매매가능수량": "1",
                    "매입가": "70000",
                    "현재가": "+71000",
                }
            ]
            next_key = ""
        result = SimpleNamespace(
            rows=rows,
            continuation_key=next_key,
            warnings=[],
            errors=[],
        )
        kwargs["on_complete"](result)
        return SimpleNamespace(accepted=True, result=result)


def _snapshot_command() -> GatewayCommand:
    idempotency_key = "live-sim-broker-snapshot:snapshot-1"
    return GatewayCommand(
        command_id="cmd-snapshot-1",
        command_type="broker_snapshot_request",
        source="live_sim",
        idempotency_key=idempotency_key,
        payload={
            "snapshot_id": "snapshot-1",
            "account_id": "99991234",
            "trade_date": market_today(),
            "account_mode": "SIMULATION",
            "broker_env": "SIMULATION",
            "server_mode": "SIMULATION",
            "stale_after_sec": 120,
            "max_pages_per_section": 20,
            "idempotency_key": idempotency_key,
            "mode": "LIVE_SIM",
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "read_only": True,
            "automatic_local_repair": False,
        },
    )


def test_account_mask_never_returns_raw_account_id() -> None:
    assert mask_account_id("9999-1234") == "***1234"
    assert mask_account_id("1234") == "****"
    assert mask_account_id("") == "UNCONFIGURED"


def test_kiwoom_snapshot_collects_continuation_and_masks_account() -> None:
    runner = _SnapshotTrRunner()
    handler = KiwoomGatewayCommandHandler(
        _SnapshotClient(),
        tr_runner=runner,
    )

    events = handler.handle(_snapshot_command())

    assert events[0].event_type == "command_started"
    assert events[1].payload["snapshot_status"] == "REQUESTED"
    terminal = next(
        event
        for event in events
        if event.event_type == "account_snapshot"
        and event.payload["snapshot_status"] == "COMPLETE"
    )
    assert terminal.payload["account_id_masked"] == "***1234"
    assert terminal.payload["completed_sections"] == [
        "OPEN_ORDERS",
        "EXECUTIONS",
        "POSITIONS",
    ]
    assert len(terminal.payload["page_lineage"]) == 4
    assert terminal.payload["open_orders"][0]["filled_quantity"] == 1
    assert terminal.payload["executions"][0]["quantity"] == 1
    assert terminal.payload["positions"][0]["code"] == "005930"
    assert "99991234" not in json.dumps(terminal.payload, ensure_ascii=False)
    assert runner.calls[1]["continuation_key"] == "2"
    assert events[-1].event_type == "command_ack"


def test_kiwoom_snapshot_real_server_fails_before_any_tr_request() -> None:
    runner = _SnapshotTrRunner()
    handler = KiwoomGatewayCommandHandler(
        _SnapshotClient(server_gubun="0"),
        tr_runner=runner,
    )

    events = handler.handle(_snapshot_command())

    assert [event.event_type for event in events] == [
        "command_started",
        "command_failed",
    ]
    assert runner.calls == []
    assert "requires simulation server" in events[-1].payload["error_message"]


def test_kiwoom_snapshot_page_limit_is_incomplete_and_fail_closed() -> None:
    runner = _SnapshotTrRunner()
    original = _snapshot_command()
    payload = dict(original.payload)
    payload["max_pages_per_section"] = 1
    command = GatewayCommand(
        command_id=original.command_id,
        command_type=original.command_type,
        source=original.source,
        idempotency_key=original.idempotency_key,
        payload=payload,
    )
    handler = KiwoomGatewayCommandHandler(_SnapshotClient(), tr_runner=runner)

    events = handler.handle(command)

    terminal = next(
        event
        for event in events
        if event.event_type == "account_snapshot"
        and event.payload["snapshot_status"] == "INCOMPLETE"
    )
    assert terminal.payload["complete"] is False
    assert terminal.payload["errors"] == ["SNAPSHOT_PAGE_LIMIT:OPEN_ORDERS:1"]
    assert len(runner.calls) == 1
    assert events[-1].event_type == "command_failed"


def test_snapshot_request_queues_once_and_keeps_response_masked(tmp_path) -> None:
    connection = initialize_database(tmp_path / "snapshot-request.sqlite3")
    settings = _live_sim_settings(
        live_sim_reconcile_request_broker_snapshot_enabled=True,
    )

    request = request_live_sim_broker_snapshot(
        connection,
        settings=settings,
        snapshot_id="snapshot-request-1",
    )
    with pytest.raises(ValueError, match="BROKER_SNAPSHOT_REQUEST_ALREADY_ACTIVE"):
        request_live_sim_broker_snapshot(
            connection,
            settings=settings,
            snapshot_id="snapshot-request-2",
        )
    connection.execute(
        "UPDATE gateway_commands SET status = 'ACKED' "
        "WHERE command_type = 'broker_snapshot_request'"
    )
    connection.commit()
    with pytest.raises(
        ValueError,
        match="BROKER_SNAPSHOT_REQUEST_DUPLICATE_SNAPSHOT_ID",
    ):
        request_live_sim_broker_snapshot(
            connection,
            settings=settings,
            snapshot_id="snapshot-request-1",
        )
    commands = poll_commands(connection)
    connection.close()

    assert request["snapshot_status"] == "REQUESTED"
    assert request["account_id_masked"].endswith("5678")
    assert settings.live_sim_account_id not in json.dumps(request)
    assert commands == []
    stored = initialize_database(tmp_path / "snapshot-request.sqlite3")
    command = stored.execute(
        "SELECT command_type, payload_json FROM gateway_commands"
    ).fetchone()
    stored.close()
    assert command["command_type"] == "broker_snapshot_request"
    payload = json.loads(command["payload_json"])
    assert payload["read_only"] is True
    assert payload["live_real_allowed"] is False


def test_complete_snapshot_matches_and_stale_snapshot_blocks_new_buy(tmp_path) -> None:
    connection = initialize_database(tmp_path / "snapshot-reconcile.sqlite3")
    settings = _live_sim_settings(
        live_sim_reconcile_request_broker_snapshot_enabled=True,
        live_sim_broker_snapshot_stale_sec=120,
    )
    trade_date = market_today()
    fresh = {
        "snapshot_id": "fresh-empty",
        "snapshot_status": "COMPLETE",
        "complete": True,
        "account_id_masked": "***5678",
        "trade_date": trade_date,
        "snapshot_at": datetime_to_wire(utc_now()),
        "stale_after_sec": 120,
        "requested_sections": ["OPEN_ORDERS", "EXECUTIONS", "POSITIONS"],
        "completed_sections": ["OPEN_ORDERS", "EXECUTIONS", "POSITIONS"],
        "open_orders": [],
        "executions": [],
        "positions": [],
        "page_lineage": [],
        "errors": [],
        "source": "fixture",
    }

    matched = reconcile_live_sim(connection, settings=settings, broker_snapshot=fresh)
    stale = reconcile_live_sim(
        connection,
        settings=settings,
        broker_snapshot={
            **fresh,
            "snapshot_id": "stale-empty",
            "snapshot_at": "2026-07-01T00:00:00Z",
        },
    )
    wrong_identity = reconcile_live_sim(
        connection,
        settings=settings,
        broker_snapshot={
            **fresh,
            "snapshot_id": "wrong-identity",
            "account_id_masked": "***9999",
            "trade_date": "2026-07-01",
        },
    )
    connection.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id, candidate_instance_id, strategy_observation_id,
            risk_observation_id, trade_date, account_id, code, name, side,
            order_type, quantity, limit_price, notional, status,
            idempotency_key, created_at
        ) VALUES (
            'blocked-intent', 'candidate-1', 'strategy-1', 'risk-1', ?,
            'SIM-12345678', '005930', '삼성전자', 'BUY', 'LIMIT', 1,
            70000, 70000, 'CREATED', 'blocked-intent-key', ?
        )
        """,
        (trade_date, datetime_to_wire(utc_now())),
    )
    connection.commit()
    with pytest.raises(ValueError, match="LIVE_SIM_RECONCILE_MISMATCH_BLOCK"):
        queue_live_sim_order_command(
            connection,
            "blocked-intent",
            settings=settings,
        )
    send_order_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands WHERE command_type = 'send_order'"
    ).fetchone()["count"]
    blocked = _latest_reconcile_blocks_new_buy(connection, settings)
    connection.close()

    assert matched.status == "OK"
    assert matched.account_id == "***5678"
    assert matched.snapshot_json["broker_snapshot"]["fresh"] is True
    assert stale.status == "RECONCILE_MISMATCH"
    assert stale.snapshot_json["blocking_new_buy"] is True
    assert "broker_snapshot_stale" in {
        item["reason"] for item in stale.snapshot_json["mismatches"]
    }
    assert {
        "broker_snapshot_account_mismatch",
        "broker_snapshot_trade_date_mismatch",
    }.issubset(
        {item["reason"] for item in wrong_identity.snapshot_json["mismatches"]}
    )
    assert blocked is True
    assert send_order_count == 0


def test_order_execution_and_position_snapshot_match_then_detect_deltas(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "snapshot-deltas.sqlite3")
    settings = _live_sim_settings(
        live_sim_reconcile_request_broker_snapshot_enabled=True,
        live_sim_reconcile_notional_tolerance=1.0,
    )
    trade_date = market_today()
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO gateway_commands (
            command_id, command_type, source, status, idempotency_key,
            payload_json, payload_hash, created_at
        ) VALUES (
            'cmd-local-1', 'send_order', 'live_sim', 'ACKED', 'idem-local-1',
            '{}', 'hash-local-1', ?
        )
        """,
        (now,),
    )
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id, live_sim_intent_id, gateway_command_id, trade_date,
            account_id, code, name, side, order_type, quantity, limit_price,
            notional, status, broker_order_no, filled_quantity,
            remaining_quantity, idempotency_key, created_at
        ) VALUES (
            'local-order-1', 'local-intent-1', 'cmd-local-1', ?,
            'SIM-12345678', '005930', '삼성전자', 'BUY', 'LIMIT', 2, 70000,
            140000, 'PARTIALLY_FILLED', 'BROKER-1', 1, 1, 'order-local-1', ?
        )
        """,
        (trade_date, now),
    )
    connection.execute(
        """
        INSERT INTO live_sim_executions (
            live_sim_execution_id, broker_execution_id, execution_key,
            live_sim_order_id, live_sim_intent_id, broker_order_no, account_id,
            code, side, quantity, price, notional, executed_at, raw_event_json
        ) VALUES (
            'local-exec-1', 'EXEC-1', 'exec-key-1', 'local-order-1',
            'local-intent-1', 'BROKER-1', 'SIM-12345678', '005930', 'BUY',
            1, 70000, 70000, ?, '{}'
        )
        """,
        (now,),
    )
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id, account_id, trade_date, code, name, quantity,
            available_quantity, avg_entry_price, total_entry_notional,
            opened_at, status, source_live_sim_order_id,
            source_live_sim_intent_id, created_at, updated_at
        ) VALUES (
            'position-1', 'SIM-12345678', ?, '005930', '삼성전자', 1,
            1, 70000, 70000, ?, 'OPEN', 'local-order-1', 'local-intent-1', ?, ?
        )
        """,
        (trade_date, now, now, now),
    )
    connection.commit()
    broker_snapshot = {
        "snapshot_id": "matched-state",
        "snapshot_status": "COMPLETE",
        "complete": True,
        "account_id_masked": "***5678",
        "trade_date": trade_date,
        "snapshot_at": now,
        "stale_after_sec": 120,
        "requested_sections": ["OPEN_ORDERS", "EXECUTIONS", "POSITIONS"],
        "completed_sections": ["OPEN_ORDERS", "EXECUTIONS", "POSITIONS"],
        "open_orders": [
            {
                "broker_order_no": "BROKER-1",
                "code": "005930",
                "side": "BUY",
                "order_status": "PARTIALLY_FILLED",
                "quantity": 2,
                "filled_quantity": 1,
                "remaining_quantity": 1,
                "price": 70000,
            }
        ],
        "executions": [
            {
                "broker_execution_id": "EXEC-1",
                "broker_order_no": "BROKER-1",
                "code": "005930",
                "side": "BUY",
                "quantity": 1,
                "price": 70000,
                "executed_at": now,
            }
        ],
        "positions": [
            {
                "code": "005930",
                "name": "삼성전자",
                "quantity": 1,
                "available_quantity": 1,
                "avg_entry_price": 70000,
            }
        ],
        "page_lineage": [],
        "errors": [],
        "source": "fixture",
    }

    matched = reconcile_live_sim(
        connection,
        settings=settings,
        broker_snapshot=broker_snapshot,
    )
    mismatched = reconcile_live_sim(
        connection,
        settings=settings,
        broker_snapshot={
            **broker_snapshot,
            "snapshot_id": "mismatched-state",
            "executions": [{**broker_snapshot["executions"][0], "quantity": 2}],
            "positions": [
                {**broker_snapshot["positions"][0], "avg_entry_price": 71000}
            ],
        },
    )
    connection.close()

    assert matched.status == "OK"
    assert matched.mismatch_count == 0
    mismatch_reasons = {
        item["reason"] for item in mismatched.snapshot_json["mismatches"]
    }
    assert "broker_execution_sum_mismatch" in mismatch_reasons
    assert "broker_position_average_price_mismatch" in mismatch_reasons
    assert mismatched.snapshot_json["blocking_new_buy"] is True
    assert mismatched.snapshot_json["allow_exit"] is True
