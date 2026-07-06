from __future__ import annotations

import json

import pytest
from domain.broker.utils import datetime_to_wire, market_today, utc_now
from domain.live_sim.status import LiveSimIntentStatus, LiveSimOrderStatus
from services.config import Settings, TradingMode
from storage.gateway_command_store import GatewayCommandStatus, canonical_json, hash_payload_json
from storage.sqlite import initialize_database, open_connection
from tools.resolve_live_sim_order import (
    OPERATOR_BROKER_ABSENT_REASON,
    OPERATOR_BROKER_CANCELLED_REASON,
    ResolveLiveSimOrderError,
    resolve_live_sim_order,
    summarize_live_sim_order,
)


def test_resolve_live_sim_order_summary_reports_command_and_event_evidence(tmp_path) -> None:
    connection = initialize_database(tmp_path / "resolve-summary.sqlite3")
    _insert_stuck_live_sim_order(connection)

    summary = summarize_live_sim_order(connection, "order-unconfirmed")
    connection.close()

    assert summary["order"]["live_sim_order_id"] == "order-unconfirmed"
    assert summary["gateway_command"]["status"] == GatewayCommandStatus.UNCONFIRMED.value
    assert summary["broker_order_no"] is None
    assert summary["gateway_event_summary"]["blocking_event_counts"]["command_started"] == 0
    assert summary["operator_resolution_supported"]["supported"] is True


def test_resolve_broker_absent_marks_order_failed_and_reconcile_clean(tmp_path) -> None:
    connection = initialize_database(tmp_path / "resolve-broker-absent.sqlite3")
    _insert_stuck_live_sim_order(connection)

    result = resolve_live_sim_order(
        connection,
        order_id="order-unconfirmed",
        mode="broker-absent",
        note="HTS 미접수 확인",
        settings=_settings(),
    )

    order = connection.execute(
        "SELECT status, remaining_quantity, broker_message FROM live_sim_orders"
    ).fetchone()
    intent = connection.execute("SELECT status, broker_order_sent FROM live_sim_intents").fetchone()
    event = connection.execute(
        """
        SELECT event_type, status, reason, evidence_json
        FROM live_sim_lifecycle_events
        WHERE event_type = 'OPERATOR_ORDER_RESOLVED'
        """
    ).fetchone()
    connection.close()

    assert result["resolved"] is True
    assert result["reconcile"]["mismatch_count"] == 0
    assert result["reconcile"]["blocking_new_buy"] is False
    assert order["status"] == LiveSimOrderStatus.FAILED.value
    assert order["remaining_quantity"] == 0
    assert order["broker_message"] == OPERATOR_BROKER_ABSENT_REASON
    assert intent["status"] == LiveSimIntentStatus.REJECTED.value
    assert intent["broker_order_sent"] == 0
    assert event["status"] == LiveSimOrderStatus.FAILED.value
    assert event["reason"] == OPERATOR_BROKER_ABSENT_REASON
    assert json.loads(event["evidence_json"])["operator_note"] == "HTS 미접수 확인"


def test_resolve_broker_cancelled_allows_broker_order_no(tmp_path) -> None:
    connection = initialize_database(tmp_path / "resolve-broker-cancelled.sqlite3")
    _insert_stuck_live_sim_order(connection, broker_order_no="1234567")

    result = resolve_live_sim_order(
        connection,
        order_id="order-unconfirmed",
        mode="broker-cancelled",
        note="HTS 직접 취소 확인",
        settings=_settings(),
    )

    order = connection.execute(
        "SELECT status, broker_order_no, broker_message FROM live_sim_orders"
    ).fetchone()
    intent = connection.execute("SELECT status FROM live_sim_intents").fetchone()
    connection.close()

    assert result["reason"] == OPERATOR_BROKER_CANCELLED_REASON
    assert order["status"] == LiveSimOrderStatus.FAILED.value
    assert order["broker_order_no"] == "1234567"
    assert order["broker_message"] == OPERATOR_BROKER_CANCELLED_REASON
    assert intent["status"] == LiveSimIntentStatus.CANCELLED.value


def test_resolve_rejects_when_command_started_event_exists(tmp_path) -> None:
    connection = initialize_database(tmp_path / "resolve-started-reject.sqlite3")
    _insert_stuck_live_sim_order(connection)
    connection.execute(
        """
        INSERT INTO gateway_command_events (
            command_id, event_type, status, payload_json, created_at
        )
        VALUES ('cmd-unconfirmed', 'command_started', 'DISPATCHED', '{}', ?)
        """,
        (datetime_to_wire(utc_now()),),
    )
    connection.commit()

    with pytest.raises(ResolveLiveSimOrderError, match="command_started"):
        resolve_live_sim_order(
            connection,
            order_id="order-unconfirmed",
            mode="broker-absent",
            note="HTS 미접수 확인",
            settings=_settings(),
        )
    connection.close()


def test_resolve_live_real_settings_are_forbidden(tmp_path) -> None:
    connection = initialize_database(tmp_path / "resolve-live-real-reject.sqlite3")
    _insert_stuck_live_sim_order(connection)

    with pytest.raises(ResolveLiveSimOrderError, match="LIVE_REAL"):
        resolve_live_sim_order(
            connection,
            order_id="order-unconfirmed",
            mode="broker-absent",
            note="HTS 미접수 확인",
            settings=_settings(trading_mode=TradingMode.LIVE_REAL),
        )
    connection.close()


def test_resolve_reports_database_lock_without_traceback(tmp_path) -> None:
    db_path = tmp_path / "resolve-db-lock.sqlite3"
    connection = initialize_database(db_path)
    _insert_stuck_live_sim_order(connection)
    connection.execute("BEGIN IMMEDIATE")
    blocked = open_connection(db_path)
    blocked.execute("PRAGMA busy_timeout=1")

    with pytest.raises(ResolveLiveSimOrderError, match="데이터베이스가 잠겨"):
        resolve_live_sim_order(
            blocked,
            order_id="order-unconfirmed",
            mode="broker-absent",
            note="HTS 미접수 확인",
            settings=_settings(),
            db_lock_retry_sec=0.01,
        )

    blocked.close()
    connection.rollback()
    connection.close()


def _insert_stuck_live_sim_order(
    connection,
    *,
    command_status: str = GatewayCommandStatus.UNCONFIRMED.value,
    broker_order_no: str | None = None,
) -> None:
    now = datetime_to_wire(utc_now())
    payload_json = canonical_json(
        {
            "mode": "LIVE_SIM",
            "live_mode": "LIVE_SIM",
            "live_sim_only": True,
            "live_real_allowed": False,
            "metadata": {"live_sim_only": True, "live_real_allowed": False},
        }
    )
    connection.execute(
        """
        INSERT INTO gateway_commands (
            command_id,
            command_type,
            source,
            status,
            idempotency_key,
            payload_json,
            payload_hash,
            created_at,
            dispatched_at,
            expires_at,
            attempts,
            last_error
        )
        VALUES (?, 'send_order', 'live_sim', ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            "cmd-unconfirmed",
            command_status,
            "idem-unconfirmed",
            payload_json,
            hash_payload_json(payload_json),
            now,
            now,
            now,
            "Gateway order dispatch timed out; reconciliation required.",
        ),
    )
    connection.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id,
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
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
            reason_codes_json,
            evidence_json,
            idempotency_key,
            gateway_command_id,
            broker_order_sent,
            created_at,
            expires_at
        )
        VALUES (
            'intent-unconfirmed',
            'candidate-1',
            'strategy-1',
            'risk-1',
            ?,
            'SIM-12345678',
            '005930',
            '삼성전자',
            'BUY',
            'LIMIT',
            1,
            70000,
            70000,
            'COMMAND_QUEUED',
            '[]',
            '{}',
            'idem-unconfirmed',
            'cmd-unconfirmed',
            0,
            ?,
            ?
        )
        """,
        (market_today(), now, now),
    )
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
            idempotency_key,
            created_at,
            command_queued_at
        )
        VALUES (
            'order-unconfirmed',
            'intent-unconfirmed',
            'cmd-unconfirmed',
            ?,
            'SIM-12345678',
            '005930',
            '삼성전자',
            'BUY',
            'LIMIT',
            1,
            70000,
            70000,
            'COMMAND_QUEUED',
            ?,
            0,
            1,
            'idem-unconfirmed',
            ?,
            ?
        )
        """,
        (market_today(), broker_order_no, now, now),
    )
    connection.commit()


def _settings(**overrides) -> Settings:
    values = {
        "trading_mode": TradingMode.LIVE_SIM,
        "trading_allow_live_sim": True,
        "live_sim_enabled": True,
        "live_sim_order_routing_enabled": True,
        "live_sim_gateway_command_enabled": True,
        "live_sim_account_id": "SIM-12345678",
        "live_sim_kill_switch": False,
    }
    values.update(overrides)
    return Settings(**values)
