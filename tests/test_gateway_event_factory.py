from __future__ import annotations

from domain.broker.commands import GatewayCommand
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.tr import BrokerTrResponse
from gateway import event_factory
from gateway.event_factory import (
    make_command_ack_event,
    make_command_failed_event,
    make_command_started_event,
    make_condition_event,
    make_condition_load_result_event,
    make_heartbeat_event,
    make_price_tick_event,
    make_tr_response_event,
)


def test_make_heartbeat_event() -> None:
    event = make_heartbeat_event(source="test-gateway", sequence=7)

    assert event.event_type == "heartbeat"
    assert event.source == "test-gateway"
    assert event.payload["status"] == "ok"
    assert event.payload["sequence"] == 7


def test_make_price_tick_event_validates_as_broker_price_tick() -> None:
    event = make_price_tick_event(code="005930", name="삼성전자", price=71000)

    tick = BrokerPriceTick.from_dict(event.payload)

    assert event.event_type == "price_tick"
    assert tick.code == "005930"
    assert tick.price == 71000


def test_make_condition_event_validates_as_broker_condition_event() -> None:
    event = make_condition_event(code="005930", action="ENTER")

    condition_event = BrokerConditionEvent.from_dict(event.payload)

    assert event.event_type == "condition_event"
    assert condition_event.code == "005930"
    assert condition_event.action.value == "ENTER"


def test_make_tr_response_event_validates_as_broker_tr_response() -> None:
    event = make_tr_response_event(
        request_id="tr_1",
        tr_code="opt10001",
        request_name="stock_basic",
        rows=[{"code": "005930", "price": 70000}],
    )

    response = BrokerTrResponse.from_dict(event.payload)

    assert event.event_type == "tr_response"
    assert response.tr_code == "OPT10001"
    assert response.rows == [{"code": "005930", "price": 70000}]


def test_make_condition_load_result_event() -> None:
    event = make_condition_load_result_event()

    assert event.event_type == "condition_load_result"
    assert event.payload["success"] is True
    assert event.payload["conditions"][0]["condition_id"] == "mock_condition_1"


def test_command_lifecycle_event_builders() -> None:
    command = GatewayCommand(
        command_id="cmd_lifecycle",
        command_type="heartbeat_request",
        source="core",
        payload={},
        idempotency_key="idem-1",
    )

    started = make_command_started_event(command)
    ack = make_command_ack_event(command)
    failed = make_command_failed_event(command, "bad command")

    assert started.event_type == "command_started"
    assert ack.event_type == "command_ack"
    assert failed.event_type == "command_failed"
    assert failed.payload["error_message"] == "bad command"
    assert failed.command_id == "cmd_lifecycle"
    assert failed.idempotency_key == "idem-1"


def test_execution_event_factory_is_not_exposed_for_default_mock_loop() -> None:
    assert not hasattr(event_factory, "make_execution_event")

