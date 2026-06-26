from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from domain.broker.commands import GatewayCommand
from domain.broker.conditions import BrokerConditionEvent, ConditionEventAction
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.orders import (
    BrokerExecutionEvent,
    BrokerOrderRequest,
    BrokerOrderResult,
    OrderResultStatus,
    OrderSide,
    OrderType,
)
from domain.broker.tr import BrokerTrRequest, BrokerTrResponse
from domain.broker.utils import BrokerValidationError

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def price_tick_dict() -> dict[str, Any]:
    return {
        "code": "A005930",
        "name": "Samsung Electronics",
        "price": "71,000",
        "change_rate": "+1.25%",
        "volume": "1,234,567",
        "trade_value": "87654321000",
        "execution_strength": "104.5",
        "best_bid": "70,900",
        "best_ask": "71,000",
        "spread_ticks": "1",
        "day_high": "72,000",
        "day_low": "69,500",
        "trade_time": TS.isoformat(),
        "ts": TS.isoformat(),
    }


def assert_round_trip(model: Any, loader: Callable[[dict[str, Any]], Any]) -> None:
    assert loader(model.to_dict()) == model


def test_gateway_event_round_trips_and_preserves_idempotency_key() -> None:
    tick = BrokerPriceTick.from_dict(price_tick_dict())
    event = GatewayEvent(
        event_type="price_tick",
        source="kiwoom-gateway",
        payload=tick.to_dict(),
        command_id="cmd_condition_sync",
        idempotency_key="tick-005930-20260626T090102",
        ts=TS,
    )

    assert event.event_id.startswith("evt_")
    assert event.to_dict()["payload"]["price"] == 71000
    assert_round_trip(event, GatewayEvent.from_dict)
    assert GatewayEvent.from_dict(event.to_dict()).idempotency_key == event.idempotency_key


def test_gateway_command_round_trips_and_generates_command_id() -> None:
    order = BrokerOrderRequest(
        code="005930",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10,
        price=71000,
        idempotency_key="buy-005930-once",
        ts=TS,
    )
    command = GatewayCommand(
        command_type="submit_order",
        source="core",
        payload=order.to_dict(),
        idempotency_key="buy-005930-once",
        ts=TS,
    )

    assert command.command_id.startswith("cmd_")
    assert_round_trip(command, GatewayCommand.from_dict)
    assert GatewayCommand.from_dict(command.to_dict()).idempotency_key == "buy-005930-once"


def test_payload_models_round_trip_serialization() -> None:
    models: list[tuple[Any, Callable[[dict[str, Any]], Any]]] = [
        (BrokerPriceTick.from_dict(price_tick_dict()), BrokerPriceTick.from_dict),
        (
            BrokerOrderRequest(
                code="005930",
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity=3,
                price=72000,
                client_order_id="order_1",
                account_id="acc-1",
                idempotency_key="sell-005930-1",
                ts=TS,
            ),
            BrokerOrderRequest.from_dict,
        ),
        (
            BrokerOrderResult(
                client_order_id="order_1",
                code="005930",
                accepted=True,
                status=OrderResultStatus.ACCEPTED,
                broker_order_id="broker-1",
                command_id="cmd_1",
                idempotency_key="sell-005930-1",
                ts=TS,
            ),
            BrokerOrderResult.from_dict,
        ),
        (
            BrokerExecutionEvent(
                execution_id="exec-1",
                broker_order_id="broker-1",
                client_order_id="order_1",
                code="005930",
                side=OrderSide.SELL,
                quantity=3,
                price=72000,
                executed_at=TS,
                remaining_quantity=0,
                commission=12.5,
                tax=30.0,
                ts=TS,
            ),
            BrokerExecutionEvent.from_dict,
        ),
        (
            BrokerTrRequest(
                request_id="tr-1",
                tr_code="opt10001",
                request_name="stock_basic",
                params={"code": "005930"},
                continuation_key="0",
                ts=TS,
            ),
            BrokerTrRequest.from_dict,
        ),
        (
            BrokerTrResponse(
                request_id="tr-1",
                tr_code="opt10001",
                request_name="stock_basic",
                success=True,
                rows=[{"code": "005930", "price": 71000}],
                message="ok",
                ts=TS,
            ),
            BrokerTrResponse.from_dict,
        ),
        (
            BrokerConditionEvent(
                condition_id="cond-1",
                condition_name="breakout",
                code="005930",
                name="Samsung Electronics",
                action=ConditionEventAction.ENTER,
                price=71000,
                metadata={"rank": 1},
                ts=TS,
            ),
            BrokerConditionEvent.from_dict,
        ),
    ]

    for model, loader in models:
        assert_round_trip(model, loader)


def test_missing_required_fields_raise_validation_error() -> None:
    with pytest.raises(BrokerValidationError, match="payload"):
        GatewayEvent.from_dict({"event_type": "heartbeat", "source": "gateway"})

    with pytest.raises(BrokerValidationError, match="code"):
        BrokerPriceTick.from_dict({"name": "Samsung Electronics"})


def test_numeric_strings_are_normalized() -> None:
    tick = BrokerPriceTick.from_dict(price_tick_dict())
    order = BrokerOrderRequest.from_dict(
        {
            "code": "005930",
            "side": "buy",
            "order_type": "limit",
            "quantity": "10",
            "price": "71,000",
            "ts": TS.isoformat(),
        }
    )

    assert tick.code == "005930"
    assert tick.price == 71000
    assert tick.change_rate == 1.25
    assert tick.volume == 1234567
    assert order.side is OrderSide.BUY
    assert order.quantity == 10
    assert order.price == 71000


@pytest.mark.parametrize(
    "factory",
    [
        lambda: BrokerPriceTick.from_dict({**price_tick_dict(), "code": "ABC"}),
        lambda: BrokerPriceTick.from_dict({**price_tick_dict(), "price": "-1"}),
        lambda: BrokerOrderRequest(code="005930", side=OrderSide.BUY, quantity=0, price=71000),
        lambda: BrokerOrderRequest(code="005930", side=OrderSide.BUY, quantity=1, price=-1),
        lambda: BrokerOrderRequest(code="005930", side=OrderSide.BUY, quantity=1, price=None),
    ],
)
def test_invalid_code_quantity_and_price_are_rejected(factory: Callable[[], object]) -> None:
    with pytest.raises(ValueError):
        factory()
